package pdc;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.*;
import java.nio.ByteBuffer;

/**
 * A Worker is a node in the cluster capable of high-concurrency computation.
 * 
 * Handles task execution, result reporting, and heartbeat responses.
 */
public class Worker {
    private Socket masterSocket;
    private DataInputStream dis;
    private DataOutputStream dos;
    private String workerId;
    private volatile boolean connected = false;
    private volatile boolean running = false;
    
    // Thread pool for executing tasks
    private final ExecutorService executorService = Executors.newFixedThreadPool(4);
    
    // Student ID from environment
    private final String studentId = System.getenv("STUDENT_ID") != null ? 
        System.getenv("STUDENT_ID") : "default-student";

    public Worker() {
        this("worker-default", "localhost", 9999);
    }

    public Worker(String workerId, String masterHost, int masterPort) {
        this.workerId = workerId;
    }

    /**
     * Connects to the Master and initiates the registration handshake.
     */
    public void joinCluster(String masterHost, int port) {
        try {
            masterSocket = new Socket(masterHost, port);
            dis = new DataInputStream(masterSocket.getInputStream());
            dos = new DataOutputStream(masterSocket.getOutputStream());
            connected = true;
            
            // Send REGISTER_WORKER message
            Message registerMsg = new Message("REGISTER_WORKER", workerId, new byte[0]);
            registerMsg.writeTo(dos);
            
        } catch (IOException e) {
            System.err.println("Failed to connect to master: " + e.getMessage());
            connected = false;
        }
    }

    /**
     * Main event loop for the worker.
     */
    public void run() {
        if (!connected) {
            System.err.println("Worker not connected to master");
            return;
        }
        
        running = true;
        
        try {
            while (running && connected) {
                Message msg = Message.readFrom(dis);
                if (msg == null) {
                    break;
                }
                
                if ("RPC_REQUEST".equals(msg.messageType)) {
                    // Execute task asynchronously
                    executorService.submit(() -> processTask(msg));
                } else if ("HEARTBEAT".equals(msg.messageType)) {
                    // Respond to heartbeat
                    try {
                        Message heartbeatAck = new Message("HEARTBEAT_ACK", studentId, new byte[0]);
                        heartbeatAck.writeTo(dos);
                    } catch (IOException e) {
                        System.err.println("Failed to send heartbeat ACK: " + e.getMessage());
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("Worker communication error: " + e.getMessage());
        } finally {
            shutdown();
        }
    }

    /**
     * Executes a received task and sends the result back to master.
     */
    private void processTask(Message taskMsg) {
        try {
            ByteBuffer bb = ByteBuffer.wrap(taskMsg.payload);
            int taskIdLen = bb.get() & 0xFF;
            byte[] tid = new byte[taskIdLen];
            bb.get(tid);
            String taskId = new String(tid);
            byte op = bb.get();

            if (op == 1) { // MULTIPLY
                int rowsA = bb.getInt();
                int colsA = bb.getInt();
                int rowsB = bb.getInt();
                int colsB = bb.getInt();
                int blockStart = bb.getInt();
                int blockCount = bb.getInt();

                int[][] blockA = new int[blockCount][colsA];
                for (int i = 0; i < blockCount; i++) {
                    for (int j = 0; j < colsA; j++) {
                        blockA[i][j] = bb.getInt();
                    }
                }
                int[][] B = new int[rowsB][colsB];
                for (int i = 0; i < rowsB; i++) {
                    for (int j = 0; j < colsB; j++) {
                        B[i][j] = bb.getInt();
                    }
                }

                int[][] resultBlock = new int[blockCount][colsB];
                for (int i = 0; i < blockCount; i++) {
                    for (int j = 0; j < colsB; j++) {
                        long sum = 0;
                        for (int k = 0; k < colsA; k++) {
                            sum += (long) blockA[i][k] * (long) B[k][j];
                        }
                        resultBlock[i][j] = (int) sum;
                    }
                }

                // serialize response
                int payloadSize = 1 + taskId.length() + 4 + 4 + 4 + 4 + blockCount * colsB * 4;
                ByteBuffer out = ByteBuffer.allocate(payloadSize);
                out.put((byte) taskId.length());
                out.put(taskId.getBytes());
                out.putInt(blockStart);
                out.putInt(blockCount);
                out.putInt(blockCount);
                out.putInt(colsB);
                for (int i = 0; i < blockCount; i++) {
                    for (int j = 0; j < colsB; j++) out.putInt(resultBlock[i][j]);
                }
                Message resultMsg = new Message("TASK_COMPLETE", studentId, out.array());
                resultMsg.writeTo(dos);
                return;
            }

            sendTaskError(taskMsg, "Unknown op");
        } catch (IOException e) {
            System.err.println("Error processing task: " + e.getMessage());
        }
    }

    /**
     * Helper method to execute actual computation.
     */
    private byte[] executeComputation(String payload) {
        // Simulated computation
        Thread.currentThread().setName("Worker-" + workerId + "-Task");
        try {
            // Simulate work
            Thread.sleep(100);
            return "COMPUTED".getBytes();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return "ERROR".getBytes();
        }
    }

    /**
     * Sends an error message for a failed task.
     */
    private void sendTaskError(Message taskMsg, String errorMsg) {
        try {
            Message error = new Message("TASK_ERROR", studentId, errorMsg.getBytes());
            error.writeTo(dos);
        } catch (IOException e) {
            System.err.println("Failed to send error message: " + e.getMessage());
        }
    }

    /**
     * Helper method to execute internal task scheduling.
     */
    public void execute() {
        run();
    }

    /**
     * Shutdown the worker gracefully.
     */
    public void shutdown() {
        running = false;
        try {
            if (masterSocket != null && !masterSocket.isClosed()) {
                masterSocket.close();
            }
        } catch (IOException e) {
            System.err.println("Error closing socket: " + e.getMessage());
        }
        executorService.shutdown();
        try {
            executorService.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            executorService.shutdownNow();
        }
        connected = false;
    }

    /**
     * Check if the worker is connected.
     */
    public boolean isConnected() {
        return connected;
    }
}
