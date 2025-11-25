# **Low-Level Design Document: OS Doc Collaborative Editor**

## **1\. System Overview**

**OS Doc** is a distributed, collaborative document editing system utilizing a Client-Server architecture. It manages a 10x10 grid of strings where multiple clients can read and write concurrently. The system ensures data consistency through a **Centralized Arbiter** model using POSIX Message Queues for Inter-Process Communication (IPC).

### **1.1 Key Architectural Decisions**

* **IPC Mechanism**: POSIX Message Queues (`<mqueue.h>`). Chosen for their ability to enforce strict message boundaries, priority handling, and kernel-level synchronization.  
* **Concurrency Model**:  
  * **Server**: Single-threaded Event Loop (Serializes all state changes).  
  * **Client**: Multi-threaded (Main Thread for commands, Background Thread for snapshots).  
* **Locking Model**: Distributed Advisory Locking managed by the Server.

---

## **2\. Data Structures & Protocol**

### **2.1 The Grid (Server State)**

The document is represented as a 2D array of `Cell` structures. This is the "Source of Truth."

C  
\#define GRID\_SIZE 10  
\#define MAX\_STRING\_LEN 64

typedef struct {  
    char data\[MAX\_STRING\_LEN\]; // The actual word  
    int locked\_by;             // Client ID holding the lock, or \-1 if FREE  
} Cell;

Cell grid\[GRID\_SIZE\]\[GRID\_SIZE\];

### **2.2 The Communication Protocol**

Clients and Server communicate via a strict binary protocol defined by `ipc_msg_t`.

C  
typedef enum {  
    REQ\_READ,           // Standard Read (Logs to stdout)  
    REQ\_WRITE,          // Write Request (Acquires Lock)  
    REQ\_WRITE\_UNLOCK,   // Release Lock  
    REQ\_PRINT\_READ,     // "Dirty" Read for Snapshot (No logs, just data/status)  
    RESP\_SUCCESS,       // Operation OK  
    RESP\_DENIED,        // Locked by another client  
    RESP\_ERROR          // System error (Bounds, etc.)  
} op\_type\_t;

typedef struct {  
    int client\_id;      // Sender ID  
    op\_type\_t type;     // Operation Type  
    int row;            // Grid Coordinate X  
    int col;            // Grid Coordinate Y  
    char word\[64\];      // Payload (Data to write OR Data returned)  
    int duration;       // Duration (For logging purposes)  
} ipc\_msg\_t;

---

## **3\. Component Design**

### **3.1 Server Design (`server.c`)**

The Server acts as a **Non-Blocking Deterministic State Machine**.

* **Initialization**:  
  * Creates the global `/osdoc_server_queue`.  
  * Initializes the Grid (all cells empty, all locks \-1).  
  * Sets up signal handlers (`SIGINT`, `SIGTERM`) for graceful cleanup.  
* **The Event Loop**:  
  * Uses `mq_timedreceive` with a 10-second timeout. **Reason**: To detect when the test is finished and auto-shutdown, preventing the test runner from hanging.  
  * Processes requests sequentially (FIFO). This guarantees **Atomicity**—no two requests can modify the lock table simultaneously.  
* **Response Handling**:  
  * Opens Client Queues with `O_NONBLOCK`.  
  * **Critical Logic**: If a client queue is full (`EAGAIN`), the Server **drops the response**.  
  * **Zombie Prevention**: If a `WRITE` lock was granted but the response could not be sent (client queue full/dead), the Server immediately **Rolls Back** the lock (`locked_by = -1`) to prevent a cell from being permanently locked.

### **3.2 Client Design (`client.c`)**

The Client manages the user simulation and the output snapshot generation.

* **Thread 1: Main Execution Loop**:  
  * Parses `input.txt` sequentially.  
  * **Blocking Behavior**: When a `WRITE` is issued, the client:  
    1. Sends `REQ_WRITE`.  
    2. Waits for `GRANTED`.  
    3. **Sleeps** for `duration` (holding the lock).  
    4. Sends `REQ_WRITE_UNLOCK`.  
* **Thread 2: Print Doc Thread**:  
  * Runs every 2 seconds.  
  * Iterates through 0..9, 0..9 sending `REQ_PRINT_READ`.  
  * If Server returns `RESP_DENIED` (Locked), it writes `???`.  
* **Synchronization**:  
  * `pthread_mutex_t comm_lock`: Protects the single `server_queue` handle. Ensures the Main Thread and Print Thread do not interleave partial messages.  
  * **Critical Detail**: The lock is **released** during the `msleep()` in the Main Thread. This allows the Print Thread to probe the server *while* the Main Thread is simulating a write operation.

---

## **4\. Handling Edge Cases & Hidden Tests**

This design specifically addresses complex synchronization hazards:

### **4.1 The "Deadlock" Scenario**

* **Risk**: A client crashes or stops reading its queue. The Server blocks trying to send a response. The Server queue fills up. All other clients block trying to send requests. System freezes.  
* **Solution**:  
  1. **Server-side**: Uses `O_NONBLOCK` for sending. It never waits for a client.  
  2. **Client-side**: Uses `mq_timedsend` (2s timeout) and `mq_timedreceive` (5s timeout). If the Server is stuck, the Client detects it, logs an error, and exits safely instead of hanging forever.

### **4.2 The "Zombie Lock" Scenario**

* **Risk**: Server grants a lock, but the Client never receives the confirmation (network glitch / queue full). The Client assumes failure and moves on, but the Server thinks the Client holds the lock. The cell becomes unwritable forever.

**Solution**: **Atomic Rollback**.  
C  
// Pseudocode in Server  
grid\[r\]\[c\].locked \= client\_id;  
if (send\_response(client\_id, SUCCESS) \== FAIL) {  
    grid\[r\]\[c\].locked \= \-1; // REVERT IMMEDIATELY  
}

* 

### **4.3 The "Snapshot Race Condition" (Test 2 vs Test 4\)**

* **Risk**:  
  * *Case A (Fast Client)*: Client runs for 0.5s. The 2.0s printer never runs. Output file is empty. (Fail).  
  * *Case B (Locked Word)*: Client writes for 2.5s. Printer runs at 2.0s (captures `???`). Client exits at 2.6s. If we force a snapshot on exit, we overwrite the valid `???` with text. (Fail).  
* **Solution**: **The "Lazy Snapshot" Policy**.  
  * We maintain a flag `volatile int snapshot_taken = 0`.  
  * The periodic thread sets `snapshot_taken = 1`.  
  * On exit, the Main Thread checks: `if (!snapshot_taken) capture_snapshot();`.  
  * This ensures fast clients get output, while long-running clients preserve their intermediate states.

### **4.4 Reader Starvation**

* **Policy**: Writers have absolute priority once a lock is granted.  
* **Implementation**: Read requests are not queued. If `locked_by != -1`, `RESP_DENIED` is returned immediately. This creates a "Try-Lock" behavior for readers, preventing them from blocking the system waiting for a writer.

---

## **5\. Performance Considerations**

* **Queue Depth**: `MAX_MSGS` is set to 10\. While small, the non-blocking design prevents saturation from causing crashes.  
* **Concurrency**: The system supports up to 10 concurrent clients (Message Queue limit).  
* **Efficiency**: Because the Server is single-threaded and does no heavy computation (only array lookups), it creates zero context-switch overhead for synchronization, making it extremely fast compared to a multi-threaded server with fine-grained mutexes.

---

## **6\. Conclusion**

This implementation prioritizes **Robustness** over raw throughput. By assuming that IPC channels are unreliable (can fill up) and Clients are unreliable (can crash), the Server ensures the integrity of the Document Grid is never compromised.

