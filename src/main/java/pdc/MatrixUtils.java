package pdc;

import java.nio.ByteBuffer;

public class MatrixUtils {
    // Serialize int matrix as: rows(4) cols(4) followed by rows*cols ints (4 bytes each)
    public static byte[] serializeMatrix(int[][] m) {
        int rows = m.length;
        int cols = m.length > 0 ? m[0].length : 0;
        ByteBuffer bb = ByteBuffer.allocate(8 + rows * cols * 4);
        bb.putInt(rows);
        bb.putInt(cols);
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                bb.putInt(m[i][j]);
            }
        }
        return bb.array();
    }

    public static int[][] deserializeMatrix(byte[] data) {
        ByteBuffer bb = ByteBuffer.wrap(data);
        int rows = bb.getInt();
        int cols = bb.getInt();
        int[][] m = new int[rows][cols];
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                m[i][j] = bb.getInt();
            }
        }
        return m;
    }

    public static byte[] serializeInts(int[] arr) {
        ByteBuffer bb = ByteBuffer.allocate(4 + arr.length * 4);
        bb.putInt(arr.length);
        for (int v : arr) bb.putInt(v);
        return bb.array();
    }
}
