import java.io.*;

/**
 * Created by Ash on 4/18/16.
 */
public class FileHandle {
    private RandomAccessFile file;
    private Configuration config;

    public FileHandle(Configuration initial, int id) throws FileNotFoundException {
        config = initial;
        String pathname = "peer_" + id + "/";
        File newfile = new File(pathname);
        if (!newfile.exists()) {
            newfile.mkdirs();
        }
        file = new RandomAccessFile(pathname + initial.getFileName(), "rw");
    }

    public synchronized byte[] readFromFile(int index) throws IOException {
        int len;
        if (index == config.getTotalNumOfPieces() - 1) {
            len = config.getLastPieceSize();
        } else {
            len = config.getPieceSize();
        }
        len += 4;
        byte[] data = new byte[len];
        int offset = index * (config.getPieceSize());
        file.seek(offset);
        int noOfBytesRead = 0;
        byte[] tempIndex = Util.intToByteArray(index);
        while (noOfBytesRead < 4) {
            data[noOfBytesRead] = tempIndex[noOfBytesRead];
            noOfBytesRead += 1;
        }
        while (noOfBytesRead < len) {
            byte temp = file.readByte();
            data[noOfBytesRead] = temp;
            noOfBytesRead += 1;
        }
        return data;
    }

    public synchronized int writeToFile(byte[] data) throws IOException {
        byte[] ind = new byte[4];
        int noOfBytesWritten = 0;
        while (noOfBytesWritten < 4) {
            ind[noOfBytesWritten] = data[noOfBytesWritten];
            noOfBytesWritten += 1;
        }
        int index = Util.byteToIntArray(ind);
        int offset = index * config.getPieceSize();
        int len = data.length;
        file.seek(offset);
        while (noOfBytesWritten < len) {
            file.writeByte(data[noOfBytesWritten]);
            noOfBytesWritten += 1;
        }
        return index;
    }

    public void CopyFile(File source) throws IOException {
        InputStream input = null;
        OutputStream output = null;
        try {
            input = new FileInputStream(source);
            //output = new FileOutputStream(file);
            byte[] buf = new byte[1024];
            int bytesRead;
            while ((bytesRead = input.read(buf)) > 0) {
                //output.write(buf, 0, bytesRead);
                file.write(buf, 0, bytesRead);
            }
        } finally {
            input.close();
            //output.close();
        }
    }
}
