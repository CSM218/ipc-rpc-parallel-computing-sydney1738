package pdc;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;
import java.nio.ByteBuffer;

/**
 * The Master acts as the Coordinator in a distributed cluster.
 * 
 * Handles task distribution, failure detection, and result aggregation.
 */
public class Master {
    private final ExecutorService systemThreads = Executors.newCachedThreadPool();
    private final ExecutorService heartbeatExecutor = Executors.newSingleThreadExecutor();
    private ServerSocket serverSocket;
    private volatile boolean running = false;
    
    // Worker registry: workerId -> WorkerConnection
    private final ConcurrentHashMap<String, WorkerConnection> workers = new ConcurrentHashMap<>();
    
    // Results storage
    private final ConcurrentHashMap<String, byte[]> results = new ConcurrentHashMap<>();
    // Task tracking
    private final BlockingQueue<Task> pendingTasks = new LinkedBlockingQueue<>();
    private final ConcurrentHashMap<String, Task> assignedTasks = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Set<String>> workerTasks = new ConcurrentHashMap<>();
    
    // Heartbeat configuration
    private static final long HEARTBEAT_INTERVAL = 5000; // 5 seconds
    private static final long HEARTBEAT_TIMEOUT = 15000; // 15 seconds
    
    // Student ID from environment
    private final String studentId = System.getenv("STUDENT_ID") != null ? 
        System.getenv("STUDENT_ID") : "default-student";

    public Master() {
    }

    /**
     * Distributes computation tasks across workers.
     */
    public Object coordinate(String operation, int[][] data, int workerCount) {
        // Stub: return null for initial implementation
        // TODO: Implement actual distributed matrix operations
        return null;
    }

    /**
     * Distributed matrix multiplication driver.
     * Splits `A` into row-blocks and assigns to workers. Blocks are recombined.
     */
    public int[][] multiplyMatricesDistributed(int[][] A, int[][] B, int desiredWorkers) {
        if (A == null || B == null) return null;
        int n = A.length;
        int m = A[0].length;
        int p = B[0].length;

        // Create tasks partitioning rows of A
        int workerCount = Math.max(1, Math.min(desiredWorkers, Math.max(1, workers.size())));
        if (workerCount == 0) workerCount = 1;
        int blockSize = Math.max(1, n / workerCount);
        int taskCounter = 0;
        List<String> taskIds = new ArrayList<>();

        byte[] bData = MatrixUtils.serializeMatrix(B);

        for (int start = 0; start < n; start += blockSize) {
            int end = Math.min(n, start + blockSize);
            int blockCount = end - start;
            // prepare blockA
            int[][] blockA = new int[blockCount][m];
            for (int i = 0; i < blockCount; i++) System.arraycopy(A[start + i], 0, blockA[i], 0, m);

            ByteBuffer bb = ByteBuffer.allocate(1 + 36 + blockCount * m * 4 + bData.length);
            String taskId = "T" + (taskCounter++);
            bb.put((byte) taskId.length()); bb.put(taskId.getBytes());
            bb.put((byte)1); // op=1 multiply
            bb.putInt(blockCount); // rowsA for this block
            bb.putInt(m); // colsA
            // read rowsB and colsB from bData header
            ByteBuffer bbB = ByteBuffer.wrap(bData);
            int rowsB = bbB.getInt();
            int colsB = bbB.getInt();
            bb.putInt(rowsB);
            bb.putInt(colsB);
            bb.putInt(start); // blockStart
            bb.putInt(blockCount);
            // blockA data
            for (int i = 0; i < blockCount; i++) for (int j = 0; j < m; j++) bb.putInt(blockA[i][j]);
            // append full B matrix ints (strip header in bData)
            bb.put(bData);

            Task t = new Task(taskId, "MULTIPLY", bb.array());
            pendingTasks.offer(t);
            taskIds.add(taskId);
        }

        // Wait for all results
        int[][] result = new int[n][p];
        long deadline = System.currentTimeMillis() + 30000;
        Set<String> remaining = new HashSet<>(taskIds);
        while (!remaining.isEmpty() && System.currentTimeMillis() < deadline) {
            for (Iterator<String> it = remaining.iterator(); it.hasNext();) {
                String tid = it.next();
                byte[] res = results.get(tid);
                if (res != null) {
                    // parse result: [taskIdLen][taskId][blockStart][blockCount][rows][cols][data]
                    try {
                        ByteBuffer rb = ByteBuffer.wrap(res);
                        int tlen = rb.get() & 0xFF;
                        byte[] tbytes = new byte[tlen]; rb.get(tbytes);
                        int blockStart = rb.getInt();
                        int blockCount = rb.getInt();
                        int rows = rb.getInt();
                        int cols = rb.getInt();
                        for (int i = 0; i < blockCount; i++) {
                            for (int j = 0; j < cols; j++) result[blockStart + i][j] = rb.getInt();
                        }
                        it.remove();
                    } catch (Exception ex) {
                        // ignore parse errors for now
                    }
                }
            }
            try { Thread.sleep(50); } catch (InterruptedException e) { Thread.currentThread().interrupt(); break; }
        }

        return result;
    }

    /**
     * Start the communication listener.
     */
    public void listen(int port) throws IOException {
        this.serverSocket = new ServerSocket(port);
        this.running = true;
        
        // Start the listener thread
        systemThreads.submit(() -> acceptConnections());
        
        // Start heartbeat monitor
        heartbeatExecutor.submit(() -> monitorHeartbeats());
    }

    private void acceptConnections() {
        while (running && !systemThreads.isShutdown()) {
            try {
                Socket clientSocket = serverSocket.accept();
                systemThreads.submit(() -> handleWorkerConnection(clientSocket));
            } catch (IOException e) {
                if (running) {
                    System.err.println("Error accepting connection: " + e.getMessage());
                }
            }
        }
    }

    private void handleWorkerConnection(Socket socket) {
        try {
            DataInputStream dis = new DataInputStream(socket.getInputStream());
            DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
            
            // Read first message (should be REGISTER_WORKER)
            Message msg = Message.readFrom(dis);
            if (msg == null) {
                socket.close();
                return;
            }
            
            String workerId = msg.studentId;
            
            // Create worker connection
            WorkerConnection workerConn = new WorkerConnection(
                workerId, socket, dis, dos, System.currentTimeMillis()
            );
            workers.put(workerId, workerConn);
            
            // Send acknowledgment
            Message ack = new Message("WORKER_ACK", studentId, workerId.getBytes());
            ack.writeTo(dos);
            
            // Start a reader for incoming messages from this worker
            systemThreads.submit(() -> handleWorkerMessages(workerConn));

            // Start a sender thread to drain pending tasks to this worker
            systemThreads.submit(() -> workerSenderLoop(workerConn));
            
        } catch (IOException e) {
            System.err.println("Worker connection error: " + e.getMessage());
        } finally {
            try {
                socket.close();
            } catch (IOException e) {
                // Ignore
            }
        }
    }

    private void handleWorkerMessages(WorkerConnection worker) {
        try {
            while (running) {
                Message msg = Message.readFrom(worker.dis);
                if (msg == null) {
                    break;
                }
                
                worker.lastHeartbeat = System.currentTimeMillis();
                
                if ("TASK_COMPLETE".equals(msg.messageType)) {
                    // store result and remove assignment
                    try {
                        String taskId = new String(msg.payload, java.nio.charset.StandardCharsets.UTF_8).split(":",2)[0];
                        results.put(taskId, msg.payload);
                        Task completed = assignedTasks.remove(taskId);
                        if (completed != null) {
                            Set<String> set = workerTasks.getOrDefault(worker.id, Collections.newSetFromMap(new ConcurrentHashMap<>()));
                            set.remove(taskId);
                        }
                    } catch (Exception e) {
                        // fallback: put payload under worker id
                        results.put(worker.id, msg.payload);
                    }
                } else if ("HEARTBEAT_ACK".equals(msg.messageType)) {
                    // Update heartbeat timestamp
                    worker.lastHeartbeat = System.currentTimeMillis();
                }
            }
        } catch (IOException e) {
            System.err.println("Error communicating with worker: " + e.getMessage());
        } finally {
            workers.remove(worker.id);
            // reassign any outstanding tasks
            reassignWorkerTasks(worker.id);
        }
    }

    private void workerSenderLoop(WorkerConnection worker) {
        while (running && workers.containsKey(worker.id)) {
            Task task = null;
            try {
                task = pendingTasks.poll(500, TimeUnit.MILLISECONDS);
                if (task == null) continue;

                // assign
                assignedTasks.put(task.taskId, task);
                workerTasks.computeIfAbsent(worker.id, k -> Collections.newSetFromMap(new ConcurrentHashMap<>())).add(task.taskId);

                Message req = new Message("RPC_REQUEST", studentId, task.payload);
                req.writeTo(worker.dos);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (IOException e) {
                // couldn't send - put task back and break
                if (task != null) pendingTasks.offer(task);
                break;
            }
        }
    }

    private void reassignWorkerTasks(String workerId) {
        Set<String> tasks = workerTasks.remove(workerId);
        if (tasks == null) return;
        for (String tid : tasks) {
            Task t = assignedTasks.remove(tid);
            if (t != null) pendingTasks.offer(t);
        }
    }

    private void monitorHeartbeats() {
        while (running) {
            try {
                Thread.sleep(HEARTBEAT_INTERVAL);
                
                long currentTime = System.currentTimeMillis();
                List<String> deadWorkers = new ArrayList<>();
                
                for (WorkerConnection worker : workers.values()) {
                    if (currentTime - worker.lastHeartbeat > HEARTBEAT_TIMEOUT) {
                        deadWorkers.add(worker.id);
                    } else {
                        // Send heartbeat
                        try {
                            Message heartbeat = new Message("HEARTBEAT", studentId, new byte[0]);
                            heartbeat.writeTo(worker.dos);
                        } catch (IOException e) {
                            deadWorkers.add(worker.id);
                        }
                    }
                }
                
                // Remove dead workers and reassign their tasks
                for (String workerId : deadWorkers) {
                    WorkerConnection removed = workers.remove(workerId);
                    if (removed != null) {
                        try { removed.dos.close(); } catch (Exception ex) {}
                        try { removed.dis.close(); } catch (Exception ex) {}
                        reassignWorkerTasks(workerId);
                    }
                }
                
            } catch (InterruptedException e) {
                break;
            }
        }
    }

    /**
     * System Health Check.
     */
    public void reconcileState() {
        // Remove dead workers
        long currentTime = System.currentTimeMillis();
        for (WorkerConnection worker : workers.values()) {
            if (currentTime - worker.lastHeartbeat > HEARTBEAT_TIMEOUT) {
                workers.remove(worker.id);
            }
        }
    }

    /**
     * Shutdown the master.
     */
    public void shutdown() {
        running = false;
        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (IOException e) {
            System.err.println("Error closing server socket: " + e.getMessage());
        }
        systemThreads.shutdown();
        heartbeatExecutor.shutdown();
    }

    /**
     * Inner class to track worker connections
     */
    private static class WorkerConnection {
        String id;
        DataInputStream dis;
        DataOutputStream dos;
        long lastHeartbeat;

        WorkerConnection(String id, Socket socket, DataInputStream dis, 
                         DataOutputStream dos, long lastHeartbeat) {
            this.id = id;
            this.dis = dis;
            this.dos = dos;
            this.lastHeartbeat = lastHeartbeat;
        }
    }

    /**
     * Inner class to represent a task
     */
    private static class Task {
        String taskId;
        String type;
        byte[] payload;

        Task(String taskId, String type, byte[] payload) {
            this.taskId = taskId;
            this.type = type;
            this.payload = payload;
        }
    }
}
