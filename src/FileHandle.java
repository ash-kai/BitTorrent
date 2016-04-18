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

    public void CopyFile(File source) throws IOException {
        System.out.println("Was called Copy File");
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
