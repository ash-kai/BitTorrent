import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by sunito on 4/17/16.
 */


public class Peer implements Runnable {

    private int id;
    private int idx;
    private int N;
    private int K;
    private int p;
    private int m;
    private int noOfPieces;

    private int[] peerids;
    private Map<Integer, BitSet> bitfieldsMap;
    private Map<Integer, Boolean> handShakeMap;

    private P2PLogger peerLog;
    private Configuration config;
    private FileHandle fileHandle;

    private Set<Integer> interestedPeers;
    private Set<Integer> unchokedPeers;
    private Set<Integer> chokedPeers;
    private int optPeer;

    public Peer(int id) throws IOException {
        config = new Configuration("Common.cfg", "PeerInfo.cfg");

        this.id = id;
        N = config.getTotalPeers();
        K = config.getPrefNeighboursCount();
        p = config.getUnchokingInterval();
        m = config.getOptUnchokingIntervl();
        noOfPieces = config.getTotalNumOfPieces();
        fileHandle = new FileHandle(this.config, this.id);

        peerids = new int[N];
        bitfieldsMap = new HashMap<>();
        handShakeMap = new HashMap<>();

        peerLog = new P2PLogger(this.id);

        interestedPeers = new HashSet<>();
        unchokedPeers = new HashSet<>();
        chokedPeers = new HashSet<>();
    }

    @Override
    public void run() {

        System.out.println("Started running peer: " + id);

        List<String> hosts = config.getHostName();
        List<Integer> ports = config.getPortNumber();
        List<Integer> ids = config.getPeerID();
        for (int i = 0; i < N; i++) {
            peerids[i] = ids.get(i);
            if (peerids[i] == id) {
                idx = i;
            }
        }

        if (config.getHaveFile().get(idx)) {
            BitSet bitSet = new BitSet();
            bitSet.set(0, noOfPieces);
            bitfieldsMap.put(id, bitSet);
            try {
                File sourceFile = new File(config.getFileName());
                fileHandle.CopyFile(sourceFile);
            } catch (IOException ex) {
                System.out.println("Error: Reading Source File");
                System.out.println(ex.getMessage());
            }
        } else {
            bitfieldsMap.put(id, new BitSet());
        }

        System.out.println("BitSet: " + bitfieldsMap.get(id));

        ExecutorService clientExecutorService = Executors.newFixedThreadPool(N);
        for (int i = 0; i < N; i++) {
            if (i == idx) break;
            try {
                Socket clientSocket = new Socket(hosts.get(i), ports.get(i));
                System.out.println("connecting to server: " + hosts.get(i) + " port: " + ports.get(i));
                clientExecutorService.submit(new ClientHandler(clientSocket, peerids[i]));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        clientExecutorService.shutdown();

        ExecutorService serverExecutorService = Executors.newFixedThreadPool(N);
        try {
            ServerSocket serverSocket = new ServerSocket(ports.get(idx));
            //client TCPs count
            int TCPcount = 0;
            while (TCPcount < N - 1 - idx) {  //@TODO change logic
                serverExecutorService.submit(new ServerHandler(serverSocket.accept()));
                TCPcount++;
                System.out.println("server side TCP count: " + TCPcount);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        serverExecutorService.shutdown();
        System.out.println("shutting down Peer services " + id);
    }

    public static void main(String[] args) throws Exception {
        Peer peer = new Peer(Integer.parseInt(args[0]));
        Thread peerThread = new Thread(peer);
        peerThread.start();
    }

    class ServerHandler implements Callable<String> {

        private Socket socket;
        private int clientId;

        ServerHandler(Socket socket) {
            this.socket = socket;
        }

        @Override
        public String call() {
            int status = server_communication();
            try {
                System.out.println("SERVER: closing server socket at port " + socket.getLocalPort());
                socket.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            return "status: " + status;
        }

        //return status
        // 1 - success
        // 0 - error
        private int server_communication() {

            try (
                    OutputStream out = socket.getOutputStream();
                    InputStream in = socket.getInputStream();

            ) {

                /*
                    1. Handshake receive from client
                 */
                Handshake handshake = new Handshake();
                handshake.handShakeReceived(socket);
                clientId = handshake.getPeerId();
                handshake.setPeerId(id);
                handshake.sendHandshakeMsg(socket);
                peerLog.logTcpConnected(clientId, Calendar.getInstance());

                /*
                    2. receive bitfield from client
                 */
                Util util = new Util();
                Message msg = util.receiveMessage(in);
                bitfieldsMap.put(clientId, new BitSet());

                // Server: keep listening to messages until client gets the full file
                while (bitfieldsMap.get(clientId).cardinality() != noOfPieces) {

                    switch (msg.getType()) {


                        case (2): //interested
                            System.out.println("SERVER: Received interested msg from client: " + clientId + " to server");
                            System.out.println("SERVER: Start sending piece!! " + Util.byteToIntArray(msg.getPayload()));
                            break;

                        case (5): //bitfield
                            System.out.println("SERVER: Received bitfied msg from client: " + clientId + " to server");
                            if (msg.getPayload() != null) {
                                bitfieldsMap.put(clientId, BitSet.valueOf(msg.getPayload()));
                            }
                            Message bitfiled_msg = new Message("bitfield");
                            bitfiled_msg.setPayload(bitfieldsMap.get(id).toByteArray());
                            System.out.println("SERVER: Sending bitfield msg from sever to client");
                            util.sendMessage(out, bitfiled_msg);
                            break;

                    }

                    msg = util.receiveMessage(in);
                }
            } catch (Exception e) {
                e.printStackTrace();
                return 0;
            }
            return 1;
        }
    }

    class ClientHandler implements Callable<String> {

        private Socket socket;
        private int serverId;

        ClientHandler(Socket socket, int serverId) {
            this.socket = socket;
            this.serverId = serverId;
        }

        @Override
        public String call() {
            int status = client_communication();
            try {
                System.out.println("CLIENT: closing client socket at port " + socket.getLocalPort());
                socket.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            return "status: " + status;
        }

        //return status
        // 1 - success
        // 0 - error
        private int client_communication() {

            try (
                    OutputStream out = socket.getOutputStream();
                    InputStream in = socket.getInputStream();

            ) {
                    /*
                        1. Handshake send to server
                     */
                Handshake handshake = new Handshake();
                handshake.setPeerId(id);
                handshake.sendHandshakeMsg(socket);
                handShakeMap.put(serverId, false);
                handshake.handShakeReceived(socket);
                System.out.println("CLIENT: Handshake initiation received from " + serverId);
                if (handShakeMap.get(serverId) != null) {
                    handShakeMap.put(serverId, true);
                    peerLog.logHandshakeSuccess(serverId, Calendar.getInstance());
                    System.out.println("CLIENT: Handshake Success");
                }
                peerLog.logTcpConnection(serverId, Calendar.getInstance());

                    /*
                        2. bitfield send to server
                     */
                Util util = new Util();
                Message bitfieldMsg = new Message("bitfield");
                bitfieldMsg.setPayload(bitfieldsMap.get(id).toByteArray());
                System.out.println("CLIENT: Sending bitfied msg from client: " + id + " to server: " + serverId);
                util.sendMessage(out, bitfieldMsg);

                Message msg = util.receiveMessage(in);

                //Client: keep sending messages until we receive the full file
                while (bitfieldsMap.get(id).cardinality() != noOfPieces) {

                    switch (msg.getType()) {

                        case (4): //have

                            break;
                        case (5): //bitfield
                            System.out.println("CLIENT: received bitfield msg!!");
                            boolean interested = false;
                            if (msg.getPayload() != null) {
                                bitfieldsMap.put(serverId, BitSet.valueOf(msg.getPayload()));
                                int rndPieceNumber = util.getRandomInterestingPiece(bitfieldsMap.get(id), bitfieldsMap.get(serverId));
                                if (rndPieceNumber != -1) {
                                    Message interested_msg = new Message("interested");
                                    interested_msg.setPayload(Util.intToByteArray(rndPieceNumber));
                                    System.out.println("CLIENT: Sending interested in pieceNumber: " + rndPieceNumber + " from client: " + id + " to server: " + serverId);
                                    interested = true;
                                    util.sendMessage(out, interested_msg);
                                }
                            }
                            if (!interested) {
                                Message notinterested_msg = new Message("not interested");
                                util.sendMessage(out, notinterested_msg);
                            }
                            break;

                    }

                    msg = util.receiveMessage(in);

                }


            } catch (Exception e) {
                e.printStackTrace();
                return 0;
            }
            return 1;
        }
    }
}


