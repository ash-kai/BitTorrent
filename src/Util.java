import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ResourceBundle;

/**
 * Created by Ash on 4/17/16.
 */
public class Util {

    public void sendMessage(OutputStream out, Message msg) throws IOException {

        if (msg.getPayload() == null) {
            msg.setLength(1);
        } else {
            msg.setLength(msg.getPayload().length + 1);
        }
        out.write(intToByteArray(msg.getLength()));
        System.out.println("in sendMessage: len of msg type: " + msg.getType());
        out.write(msg.getType());


        if (msg.getPayload() != null) {
            out.write(msg.getPayload());
        }
        out.flush();
    }

    public Message receiveMessage(InputStream in) throws IOException {
        Message msg = new Message(null);

        int totalRead = 0, received = 0;

        //Read the length section of message
        byte[] lengthInByte = new byte[4];
        while (totalRead < 4) {
            received = in.read(lengthInByte, totalRead, 4 - totalRead);
            totalRead += received;
        }
        msg.setLength(byteToIntArray(lengthInByte));

        //Read the type section of message
        totalRead = 0;
        byte[] mType = new byte[1];
        System.out.println("len: " + byteToIntArray(lengthInByte) + " totalRead: " + totalRead);
        while (totalRead < 1) {
            received = in.read(mType, totalRead, 1 - totalRead);
            totalRead += received;
        }
        msg.setType(mType[0]);
        //Read the remaining payload
        byte[] mPayload;
        if (msg.getLength() > 1) {
            mPayload = new byte[msg.getLength() - 1];
        } else {
            mPayload = null;
        }
        totalRead = 0;
        while (totalRead < msg.getLength() - 1) {
            received = in.read(mPayload, totalRead, msg.getLength() - 1 - totalRead);
            totalRead += received;
        }
        msg.setPayload(mPayload);
        return msg;
    }

    public static byte[] intToByteArray(int value) {
        return new byte[]{
                (byte) ((value & 0xff000000) >> 24),
                (byte) ((value & 0x00ff0000) >> 16),
                (byte) ((value & 0x0000ff00) >> 8),
                (byte) (value & 0x000000ff)};

    }

    public static int byteToIntArray(byte[] value) {
//        int result = 0;
//        for (int i = 0; i < 4; i++) {
//            result = (result << 8) - Byte.MIN_VALUE + (int) value[i];
//        }
        int result = (value[0] << 24) & 0xFF000000 |
                (value[1] << 16) & 0x00FF0000 |
                (value[2] << 8) & 0x0000FF00 |
                (value[3] << 0) & 0x000000FF;
        return result;
    }
}
