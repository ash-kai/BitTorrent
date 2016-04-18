import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Created by Ash on 4/17/16.
 */
public class Message {

    Message(String type){
        if(type!=null)
            setMessageType(type);
    }

    //Length
    private int length;

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    //Message Type
    private byte type;

    public byte getType() {
        return type;
    }

    public void setType(byte type) {
        this.type = type;
    }

    //Message Payload
    private byte[] payload;

    public byte[] getPayload() {
        return payload;
    }

    public void setPayload(byte[] payload) {
        this.payload = payload;
        if(payload != null && payload.length > 0){
            setLength(this.payload.length + 1);
        }else{
            setLength(1);
        }
    }

    public void setMessageType(String type) {
        switch (type) {
            case "choke":
                setType((byte) 0);
                break;
            case "unchoke":
                setType((byte) 1);
                break;
            case "interested":
                setType((byte) 2);
                break;
            case "not interested":
                setType((byte) 3);
                break;
            case "have":
                setType((byte) 4);
                break;
            case "bitfield":
                setType((byte) 5);
                break;
            case "request":
                setType((byte) 6);
                break;
            case "piece":
                setType((byte) 7);
                break;
        }
    }


}
