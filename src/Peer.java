import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;

/**
 * Created by Ash on 3/13/16.
 */
public class Peer implements Runnable{
    private int id;
    private int numNeighbours;
    private BitField bitField;
    private P2PLogger peerLog;
    private Configuration config;
    private ServerSocket serverSocket;
    HashMap<Integer, Boolean> handShakeMsgs = new HashMap<Integer, Boolean>();

    public Peer(int id) {
        this.id = id;
        this.config = new Configuration("Common.cfg", "PeerInfo.cfg");
        this.peerLog = new P2PLogger(this.id);
        this.bitField = new BitField(config.getTotalNumOfPieces());
        this.numNeighbours = config.getPrefNeighboursCount();
    }

    @Override
    public void run() {
        System.out.println("Peer " + this.id + " has entered the Bit Torrent connection");

        //Checking if the peer has complete file
        for (int i = 0; i < config.getTotalPeers(); i++) {
            if (config.getPeerID().get(i) == this.id) {
                //Has all the pieces of the file
                if (config.getHaveFile().get(i)) {
                    //Setting all its bit fields to true
                    this.bitField.setBitsToTrue();
                }
            }
        }

        //Starting the server socket connection for the peer
        try {
            for (int i = 0; i < config.getTotalPeers(); i++) {
                if (config.getPeerID().get(i) == this.id) {
                    this.serverSocket = new ServerSocket(config.getPortNumber().get(i));
                    break;
                }
                //Creating client socket with other Server Sockets currently active
                Socket clientSocket = new Socket(config.getHostName().get(i), config.getPortNumber().get(i));
                Calendar cal = Calendar.getInstance();
                this.peerLog.logTcpConnection(config.getPeerID().get(i), cal);

                //Send Handshake messages
                Handshake handshakeFrmClient = new Handshake();
                handshakeFrmClient.setPeerId(this.id);
                handshakeFrmClient.sendHandshakeMsg(clientSocket);
                handShakeMsgs.put(config.getPeerID().get(i), false);
                handshakeFrmClient.handShakeReceived(clientSocket);
                System.out.println("Handshake initiation received from " + handshakeFrmClient.getPeerId());
                if (handShakeMsgs.get(handshakeFrmClient.getPeerId()) != null) {
                    handShakeMsgs.put(handshakeFrmClient.getPeerId(), true);
                    peerLog.logHandshakeSuccess(handshakeFrmClient.getPeerId(),Calendar.getInstance());
                    System.out.println("Handshake Success");
                }
            }
            while (true) {
                Socket serverS = serverSocket.accept();
                Handshake handshake = new Handshake();
                handshake.handShakeReceived(serverS);

                //Get Time Instance
                Calendar cal = Calendar.getInstance();
                SimpleDateFormat formatter = new SimpleDateFormat("HH:mm:ss");

                System.out.println(formatter.format(cal.getTime())+" Got message from Peer with id : " + handshake.getPeerId());
                handshake.setPeerId(this.id);
                handshake.sendHandshakeMsg(serverS);
            }
        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        }
    }

    public static void main(String args[]) {
        Peer peer = new Peer(Integer.parseInt(args[0]));
        Thread peerThread = new Thread(peer);
        peerThread.start();
    }
}
