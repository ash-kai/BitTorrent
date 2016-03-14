import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

/**
 * Created by Ash on 3/13/16.
 */
public class Handshake {
    private String handshakeHeader;
    private byte[] zeroBits;
    private int peerId;


    public void setPeerId(int peerId) {
        this.peerId = peerId;
    }

    public int getPeerId() {
        return peerId;
    }

    public Handshake() {
        this.handshakeHeader = "P2PFILESHARINGPROJ";
        this.zeroBits = new byte[10];
        for (int i = 0; i < 10; i++) {
            this.zeroBits[i] = 0;
        }
    }

    public void handShakeReceived(Socket s) {
        try {
            InputStream inp = s.getInputStream();
            byte[] byteID = new byte[32];
            int bytesRead;
            bytesRead = inp.read(byteID);
            int id = (byteID[28] << 24) & 0xFF000000 |
                    (byteID[29] << 16) & 0x00FF0000 |
                    (byteID[30] << 8) & 0x0000FF00 |
                    (byteID[31] << 0) & 0x000000FF;
            this.peerId = id;
        } catch (IOException ex) {
            System.out.print(ex.getMessage());
        }
    }

    public void sendHandshakeMsg(Socket s) {
        try {
            OutputStream out = s.getOutputStream();
            byte[] finalMsg = new byte[32];
            byte[] ID = intToByteArray(this.peerId);
            System.arraycopy(this.handshakeHeader.getBytes(), 0, finalMsg, 0, handshakeHeader.length());
            System.arraycopy(this.zeroBits, 0, finalMsg, this.handshakeHeader.length(), this.zeroBits.length);
            System.arraycopy(ID, 0, finalMsg, 28, 4);
            out.write(finalMsg);
        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        }

    }

    public byte[] intToByteArray(int value) {
        return new byte[]{
                (byte) (value >>> 24),
                (byte) (value >>> 16),
                (byte) (value >>> 8),
                (byte) value};
    }
}
