import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

/**
 * Created by Ash on 3/13/16.
 */
public class P2PLogger {
    private Logger logger;
    private int id;
    private FileHandler handler;
    private SimpleDateFormat dateFormatter;


    public P2PLogger(int peerID) {
        try {
            this.id = peerID;
            dateFormatter = new SimpleDateFormat("HH:mm:ss");
            logger = Logger.getLogger("peerLogger_" + peerID);
            String pathname = "peer_" + id + "/";
            handler = new FileHandler(pathname + "log_peer_" + peerID + ".log");
            SimpleFormatter format = new SimpleFormatter();
            handler.setFormatter(format);
            logger.addHandler(handler);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void OffLogger(){
        logger.setLevel(Level.OFF);
    }

    public void logTcpConnection(int peerId, Calendar currentTime) {
        logger.info(dateFormatter.format(currentTime.getTime()) + " : Peer " + this.id + " makes a connection to Peer " + peerId);
    }

    public void logTcpConnected(int peerId, Calendar currentTime) {
        logger.info(dateFormatter.format(currentTime.getTime()) + " : Peer " + this.id + " is connected from Peer " + peerId);
    }

    public void logChangePrefNeighbours(List<Integer> neighbourPeers, Calendar currentTime) {
        //String neighbours = Arrays.toString(neighbourPeers);
        String neighbours = neighbourPeers.toString();
        logger.info(dateFormatter.format(currentTime.getTime()) + " : Peer " + this.id + " has the preferred neighbors " + neighbours);
    }

    public void logChangeOptUnchokedNeighbour(int peerId, Calendar currentTime) {
        logger.info(dateFormatter.format(currentTime.getTime()) + " : Peer " + this.id + " has the optimistically unchoked neighbor " + peerId);
    }

    public void logUnchoked(int peerId, Calendar currentTime) {
        logger.info(dateFormatter.format(currentTime.getTime()) + " : Peer " + this.id + " is unchoked by " + peerId);
    }

    public void logChoked(int peerId, Calendar currentTime) {
        logger.info(dateFormatter.format(currentTime.getTime()) + " : Peer " + this.id + " is choked by " + peerId);
    }

    public void logHavePiece(int peerId, Calendar currentTime, int piece) {
        logger.info(dateFormatter.format(currentTime.getTime()) + " : Peer " + this.id + " received the ‘have’ message from " + peerId + " for the piece " + piece);
    }

    public void logInterested(int peerId, Calendar currentTime) {
        logger.info(dateFormatter.format(currentTime.getTime()) + " : Peer " + this.id + " received the ‘interested’ message from " + peerId);
    }

    public void logNotInterested(int peerId, Calendar currentTime) {
        logger.info(dateFormatter.format(currentTime.getTime()) + " : Peer " + this.id + " received the ‘not interested’ message from " + peerId);
    }

    public void logDownloadedPiece(int peerId, Calendar currentTime, int piece, int totalPieces) {
        logger.info(dateFormatter.format(currentTime.getTime()) + " : Peer " + this.id + " has downloaded the piece " + piece + " from " + peerId + ".\n Now the number of pieces it has is " + totalPieces);
    }

    public void logComleteFileDownloaded(Calendar currentTime) {
        logger.info(dateFormatter.format(currentTime.getTime()) + " : Peer " + this.id + " has downloaded the complete file.");
    }

    public void logHandshakeSuccess(int peerID, Calendar currentTime) {
        logger.info(dateFormatter.format(currentTime.getTime()) + " : Peer " + this.id + " has successfully handshaked with the peer " + peerID);
    }
}
