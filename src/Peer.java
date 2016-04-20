import java.io.*;
import java.net.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;

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

    private ServerSocket serverSocket;
    private Map<Integer, Socket> clientConnections; //used by client
    private Map<Integer, Socket> serverConnections;  //used by server

    private Set<Integer> requestedPieces; //used by client to send new request msg
    private Map<Integer, Integer> piecesSentMap; //used by server to calc download rate - refresh it every p seconds

    SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");

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

        clientConnections = new HashMap<>();
        serverConnections = new HashMap<>();

        requestedPieces = new HashSet<>();
        piecesSentMap = new HashMap<>();
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
                clientConnections.put(peerids[i], clientSocket);
                clientExecutorService.submit(new ClientHandler(clientSocket, peerids[i]));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        ExecutorService serverExecutorService = Executors.newFixedThreadPool(N);
        try {
            serverSocket = new ServerSocket(ports.get(idx));
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

        ExecutorService unchokeService = Executors.newFixedThreadPool(1);
        unchokeService.submit(new Unchoke());

        ExecutorService optUnchokeService = Executors.newFixedThreadPool(1);
        optUnchokeService.submit(new OptUnchoke());

        optUnchokeService.shutdown();
        unchokeService.shutdown();
        clientExecutorService.shutdown();
        serverExecutorService.shutdown();
        try {
            //Thread.sleep(15000);
            //closeAll();
            /*while(true){

            }*/
        } catch (Exception e) {
            e.printStackTrace();
        }
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
                System.out.println(sdf.format(Calendar.getInstance().getTime()) + " - " + "SERVER: closing server socket at port " + socket.getLocalPort());
                //socket.close(); //@TODO check
            } catch (Exception e) {
                e.printStackTrace();
            }
            return "status: " + status;
        }

        //return status
        // 1 - success
        // 0 - error
        private int server_communication() {

            try {
                OutputStream out = socket.getOutputStream();
                InputStream in = socket.getInputStream();

                /*
                    1. Handshake receive from client
                 */
                Handshake handshake = new Handshake();
                handshake.handShakeReceived(socket);
                clientId = handshake.getPeerId();
                handshake.setPeerId(id);
                handshake.sendHandshakeMsg(socket);
                peerLog.logTcpConnected(clientId, Calendar.getInstance());

                serverConnections.put(clientId, socket);

                /*
                    2. receive bitfield from client
                 */
                Util util = new Util();
                Message msg = util.receiveMessage(in);
                bitfieldsMap.put(clientId, new BitSet());

                //if server has full file; then send the bitfield to client
                if (bitfieldsMap.get(id).cardinality() == noOfPieces && msg.getType() == 5) {
                    Message bitfiled_msg = new Message("bitfield");
                    bitfiled_msg.setPayload(bitfieldsMap.get(id).toByteArray());
                    System.out.println(sdf.format(Calendar.getInstance().getTime()) + " - " + "SERVER: " + id + " with FULL FILE sending bitfield msg client: " + clientId);
                    util.sendMessage(out, bitfiled_msg);
                    msg = util.receiveMessage(in);
                }

                if (msg.getType() == 5) { //bitfield
                    System.out.println(sdf.format(Calendar.getInstance().getTime()) + " - " + "SERVER: " + id + " received bitfiled from client: " + clientId);
                    boolean interested = false;
                    if (msg.getPayload() != null) {
                        bitfieldsMap.put(clientId, BitSet.valueOf(msg.getPayload()));
                        int rndPieceNumber = util.getRandomInterestingPiece(bitfieldsMap.get(id), bitfieldsMap.get(clientId));
                        if (rndPieceNumber != -1) {
                            Message interested_msg = new Message("interested");
                            interested_msg.setPayload(Util.intToByteArray(rndPieceNumber));
                            System.out.println(sdf.format(Calendar.getInstance().getTime()) + " - " + "SERVER: " + id + " sending interested in pieceNumber: " + rndPieceNumber + " to client: " + clientId);
                            util.sendMessage(out, interested_msg);
                            interested = true;
                        }
                    }
                    if (!interested) {
                        Message notinterested_msg = new Message("not interested");
                        util.sendMessage(out, notinterested_msg);

                        System.out.println(sdf.format(Calendar.getInstance().getTime()) + " - " + "SERVER: " + id + " sending not interested to client: " + clientId);
                    }

                } else if (msg.getType() == 2) { //interested
                    interestedPeers.add(clientId);
                    peerLog.logInterested(clientId, Calendar.getInstance());
                    System.out.println(sdf.format(Calendar.getInstance().getTime()) + " - " + "SERVER: " + id + " received interested from " + clientId + " piece: " + Util.byteToIntArray(msg.getPayload()));

                    //trial to send chunk
//                    byte[] tempData = fileHandle.readFromFile(Util.byteToIntArray(msg.getPayload()));
//                    Message temp = new Message("piece");
//                    temp.setPayload(tempData);
//                    util.sendMessage(out, temp);
//                    System.out.println("The data chunk send to the client: " + clientId);

                } else if (msg.getType() == 3) { //not interested
                    if (interestedPeers.contains(clientId))
                        interestedPeers.remove(clientId);
                    peerLog.logNotInterested(clientId, Calendar.getInstance());
                    System.out.println(sdf.format(Calendar.getInstance().getTime()) + " - " + "SERVER: " + id + " received not interested from " + clientId);
                } else {
                    //should not happen
                    System.out.println(sdf.format(Calendar.getInstance().getTime()) + " - " + "SERVER: " + id + " should not happend: server is expectiing bitfield/interested/notinterested but found this msgtype: " + msg.getType() + " from client: " + clientId);
                }

                //keep listening to messages from client until we receive stop
                while(true){

                    msg = util.receiveMessage(in);

                    if(msg.getType()==6){ //request
                        int pieceNumber = Util.byteToIntArray(msg.getPayload());
                        System.out.println(sdf.format(Calendar.getInstance().getTime()) + " - " + "SERVER: " + id + " received request for piece " + pieceNumber + " from clientId: " + clientId);
                        //@TODO fetch piece from file
                        Message piece_msg = new Message("piece");
                        piece_msg.setPayload(Util.intToByteArray(pieceNumber));
                        System.out.println(sdf.format(Calendar.getInstance().getTime()) + " - " + "SERVER: " + id + " sending piece " + pieceNumber + " to clientId: " + clientId);
                        util.sendMessage(out, piece_msg);
                        piecesSentMap.put(clientId, pieceNumber);
                    }else if(msg.getType()==8){ //stop
                        System.out.println(sdf.format(Calendar.getInstance().getTime()) + " - " + "SERVER: " + id + " received STOP request from clientId: " + clientId);
                        break;
                    }else{ //unexpected message
                        System.out.println(sdf.format(Calendar.getInstance().getTime()) + " - " + "SERVER: " + id + " received inside server_communication() UNEXPECTED MSG from clientId: " + clientId);
                    }

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
                System.out.println(sdf.format(Calendar.getInstance().getTime()) + " - " + "CLIENT: " + id + " closing client socket at port " + socket.getLocalPort());
                //socket.close(); //@TODO check
            } catch (Exception e) {
                e.printStackTrace();
            }
            return "status: " + status;
        }

        //return status
        // 1 - success
        // 0 - error
        private int client_communication() {

            try {
                OutputStream out = socket.getOutputStream();
                InputStream in = socket.getInputStream();

                /*
                    1. Handshake send to server
                 */
                Handshake handshake = new Handshake();
                handshake.setPeerId(id);
                handshake.sendHandshakeMsg(socket);
                handShakeMap.put(serverId, false);
                handshake.handShakeReceived(socket);
                System.out.println(sdf.format(Calendar.getInstance().getTime()) + " - " + "CLIENT: " + id + " handshake initiation received from " + serverId);
                if (handShakeMap.get(serverId) != null) {
                    handShakeMap.put(serverId, true);
                    peerLog.logHandshakeSuccess(serverId, Calendar.getInstance());
                    System.out.println(sdf.format(Calendar.getInstance().getTime()) + " - " + "CLIENT: " + id + " handshake Success");
                }
                peerLog.logTcpConnection(serverId, Calendar.getInstance());

                /*
                    2. bitfield send to server
                 */
                Util util = new Util();
                Message bitfieldMsg = new Message("bitfield");
                bitfieldMsg.setPayload(bitfieldsMap.get(id).toByteArray());
                System.out.println(sdf.format(Calendar.getInstance().getTime()) + " - " + "CLIENT: " + id + " sending bitfied to server: " + serverId);
                util.sendMessage(out, bitfieldMsg);

                Message msg = util.receiveMessage(in);

                if (msg.getType() == 5) { //bitfield
                    System.out.println(sdf.format(Calendar.getInstance().getTime()) + " - " + "CLIENT: " + id + " received bitfield from server: " + serverId);
                    boolean interested = false;
                    if (msg.getPayload() != null) {
                        bitfieldsMap.put(serverId, BitSet.valueOf(msg.getPayload()));
                        int rndPieceNumber = util.getRandomInterestingPiece(bitfieldsMap.get(id), bitfieldsMap.get(serverId));
                        if (rndPieceNumber != -1) {
                            Message interested_msg = new Message("interested");
                            interested_msg.setPayload(Util.intToByteArray(rndPieceNumber));
                            //interested_msg.setPayload(Util.intToByteArray(0));
                            System.out.println(sdf.format(Calendar.getInstance().getTime()) + " - " + "CLIENT: " + id + " sending interested in pieceNumber: " + rndPieceNumber + " to server: " + serverId);
                            util.sendMessage(out, interested_msg);

                            /*System.out.println("Receiving from Server the chunk");
                            Message temp = util.receiveMessage(in);
                            fileHandle.writeToFile(temp.getPayload(), rndPieceNumber);
                            System.out.println("Piece written to file");*/
                            interested = true;
                        }
                    }
                    if (!interested) {
                        Message notinterested_msg = new Message("not interested");
                        util.sendMessage(out, notinterested_msg);
                        System.out.println(sdf.format(Calendar.getInstance().getTime()) + " - " + "CLIENT: " + id + " sending not interested to server: " + serverId);
                    }

                } else if (msg.getType() == 2) { //interested
                    interestedPeers.add(serverId);
                    peerLog.logInterested(serverId, Calendar.getInstance());
                    System.out.println(sdf.format(Calendar.getInstance().getTime()) + " - " + "CLIENT: " + id + " received interested from " + serverId + " piece: " + Util.byteToIntArray(msg.getPayload()));
                } else if (msg.getType() == 3) { //not interested
                    if (interestedPeers.contains(serverId))
                        interestedPeers.remove(serverId);
                    peerLog.logNotInterested(serverId, Calendar.getInstance());
                    System.out.println(sdf.format(Calendar.getInstance().getTime()) + " - " + "CLIENT: " + id + " received not interested from " + serverId);
                } else {
                    //should not happen
                    System.out.println(sdf.format(Calendar.getInstance().getTime()) + " - " + "CLIENT: " + id + " should not happen: client is expectiing bitfield/interested/notinterested but found this msgtype: " + msg.getType() + " from server: " + serverId);
                }

                //communicate till client receives full file
                while (bitfieldsMap.get(id).cardinality() != noOfPieces) {

                    msg = util.receiveMessage(in);

                    if (msg.getType() == 1) { //unchoke

                        while(msg.getType() != 0) { //until we receive choked

                            System.out.println(sdf.format(Calendar.getInstance().getTime()) + " - " + "CLIENT: " + id + " received unchoke message from server: " + serverId);
                            int pieceNumber = util.getRandomInterestingPieceNotIn(bitfieldsMap.get(id), bitfieldsMap.get(serverId), requestedPieces);
                            Message request_msg = new Message("request");
                            request_msg.setPayload(Util.intToByteArray(pieceNumber));
                            System.out.println(sdf.format(Calendar.getInstance().getTime()) + " - " + "CLIENT: " + id + " requesting pieceNumber: " + pieceNumber + " from server: " + serverId);
                            util.sendMessage(out, request_msg);

                            msg = util.receiveMessage(in);
                            if(msg.getType() == 0){
                                break; //choked
                            }
                            if (msg.getType() == 7) { //piece
                                int pnum = Util.byteToIntArray(msg.getPayload());
                                System.out.println(sdf.format(Calendar.getInstance().getTime()) + " - " + "CLIENT: " + id + " received pieceNumber: " + pnum);

                                Thread.sleep(500); //@TODO check - receive and write piece to file
                                bitfieldsMap.get(id).set(pnum);
                                requestedPieces.remove(pnum);
                                System.out.println(Calendar.getInstance().getTime() + " - " + "CLIENT: " + id + " bitset: " + bitfieldsMap.get(id).toString());
                                //@TODO send have message to all clients
                            }else{
                                System.out.println(sdf.format(Calendar.getInstance().getTime()) + " - " + "CLIENT: " + id + " received UNEXPECTED MSG from server: " + serverId);
                            }
                            msg = util.receiveMessage(in);
                        }
                        if (msg.getType() == 0) { //choke
                            System.out.println(sdf.format(Calendar.getInstance().getTime()) + " - " + "CLIENT: " + id + " received choke from server: " + serverId);
                            continue;
                        }
                    }
                }

                System.out.println(sdf.format(Calendar.getInstance().getTime()) + " - " + "CLIENT: " + id + " received FULL FILE from server: " + serverId);

                // send stop message
                if(bitfieldsMap.get(id).cardinality() == noOfPieces){
                    Message stop_msg = new Message("stop");
                    util.sendMessage(out, stop_msg);
                }


            } catch (Exception e) {
                e.printStackTrace();
                return 0;
            }
            return 1;
        }
    }

    public void closeAll() {
        try {
            for (Map.Entry<Integer, Socket> entry : clientConnections.entrySet()) {
                Socket socket = entry.getValue();
                closeSocket(socket);
            }
            for (Map.Entry<Integer, Socket> entry : serverConnections.entrySet()) {
                Socket socket = entry.getValue();
                closeSocket(socket);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void closeSocket(Socket socket) throws Exception {
        if (socket != null && !socket.isClosed()) socket.getInputStream().close();
        if (socket != null && !socket.isClosed()) socket.getOutputStream().close();
        if (socket != null && !socket.isClosed()) socket.close();
    }

    public class Unchoke implements Callable<String> {

        @Override
        public String call() {
            int status = 0;
            try {
                status = unchoke();
            } catch (Exception e) {
                e.printStackTrace();
                status = -1;
            }
            return "status: " + status;
        }

        private int unchoke() throws Exception {
            long startTime = System.currentTimeMillis();

            int round = 1;
            System.out.println(sdf.format(Calendar.getInstance().getTime()) + " - " + "SERVER: " + id + " inside unchoke()");

            while (round <= 25) { //@TODO check

                if (System.currentTimeMillis() - startTime > p * 1000) {
                    Util util = new Util();
                    System.out.println(sdf.format(Calendar.getInstance().getTime()) + " - " + "SERVER: " + id + " unchoke round: " + round);
                    startTime = System.currentTimeMillis();
                    ExecutorService requestService = Executors.newFixedThreadPool(K);
                    List<Integer> prefNeis = getPreferredClients();
                    System.out.println(sdf.format(Calendar.getInstance().getTime()) + " - " + "SERVER: " + id + " prefNeis size: " + prefNeis.size());
                    /*
                        clear data structures; to populate new data
                     */
                    piecesSentMap = new HashMap<>();

                    for (Integer nei : peerids) {
                        if (nei == id) continue;
                        if (prefNeis.contains(nei) && !unchokedPeers.contains(nei)) {
                            Message unchoke_msg = new Message("unchoke");
                            System.out.println(sdf.format(Calendar.getInstance().getTime()) + " - " + "SERVER: " + id + " send unchoke to pref nei: " + nei);
                            util.sendMessage(serverConnections.get(nei).getOutputStream(), unchoke_msg);
                            //requestService.submit(new RequestHandler(nei, p));
                            unchokedPeers.add(nei);
                        } else if (!prefNeis.contains(nei) && serverConnections.containsKey(nei)) {
                            Message choke_msg = new Message("choke");
                            System.out.println(sdf.format(Calendar.getInstance().getTime()) + " - " + "SERVER: " + id + " send choke to nei: " + nei);
                            util.sendMessage(serverConnections.get(nei).getOutputStream(), choke_msg);
                            unchokedPeers.remove(nei);
                        }
                    }
                    requestService.shutdown();
                    round++;
                }


            }
            return 0;
        }

    }

    public class RequestHandler implements Callable<String> {

        private int serverId;
        private int clientId;
        private int runtime; //in seconds

        RequestHandler(int clientId, int time) {
            this.serverId = id;
            this.clientId = clientId;
            this.runtime = time;
        }

        @Override
        public String call() {
            int status = 0;
            try {
                long startTime = System.currentTimeMillis();
                Socket socket = serverConnections.get(clientId);
                InputStream in = socket.getInputStream();
                OutputStream out = socket.getOutputStream();

                while (System.currentTimeMillis() - startTime < runtime*1000) {

                    Util util = new Util();
                    System.out.println(Calendar.getInstance().getTime() + " - " + "SERVER: " + id + " client: " + clientId);
                    Message msg = util.receiveMessage(in);

                    if (msg.getType() == 6) { //request
                        int pieceNumber = Util.byteToIntArray(msg.getPayload());
                        //@TODO fetch piece from file
                        Message piece_msg = new Message("piece");
                        piece_msg.setPayload(Util.intToByteArray(pieceNumber));
                        System.out.println(sdf.format(Calendar.getInstance().getTime()) + " - " + "SERVER: " + id + " sending piece " + pieceNumber + " to clientId: " + clientId);
                        util.sendMessage(out, piece_msg);
                        piecesSentMap.put(clientId, pieceNumber);
                    } else {
                        //TODO handle this
                        System.out.println(sdf.format(Calendar.getInstance().getTime()) + " - " + "SERVER: " + id + " inside RequestHandler clientId: " + clientId + " received unexpected messageType: " + msg.getType());
                        break;
                    }

                }
            } catch (Exception e) {
                e.printStackTrace();
                status = -1;
            }

            return "status: " + status;
        }
    }

    public class OptUnchoke implements Callable<String> {

        @Override
        public String call() {
            int status = optUnchoke();
            return "status: " + status;
        }

        private int optUnchoke() {
            long startTime = System.currentTimeMillis();

            /*while(System.currentTimeMillis() - startTime < m){

            }*/
            return 0;
        }
    }

    //select preferredNeis based on prev rounds download rate
    // or random select if server has full file
    public List<Integer> getPreferredClients() {
        int count = 0;
        List<Integer> prefNeis = new ArrayList<>();
        if (bitfieldsMap.get(id).cardinality() == noOfPieces) {
            //random select
            List<Integer> interestedPeersList = new ArrayList<>(interestedPeers);
            Collections.shuffle(interestedPeersList);
            for (Integer nei : interestedPeersList) {
                if (count == K) break;
                prefNeis.add(nei);
                count++;
            }
            return prefNeis;
        }
        List<Integer> sortedNeis = sortByDownloadRate();
        for (int i = sortedNeis.size() - 1; i >= 0; i--) {
            if (count == K) break;
            prefNeis.add(sortedNeis.get(i));
        }
        return prefNeis;
    }

    public List<Integer> sortByDownloadRate() {
        List<Integer> neis = new ArrayList<>();

        class Pair {
            int neiId;
            int freq;

            Pair(int neiId, int freq) {
                this.neiId = neiId;
                this.freq = freq;
            }

            @Override
            public int hashCode() {
                return neiId * 31 + 17 * freq;
            }

            @Override
            public boolean equals(Object obj) {
                if (!(obj instanceof Pair)) return false;
                if (this == obj) return true;
                Pair other = (Pair) obj;
                return this.neiId == other.neiId && this.freq == other.freq;
            }
        }

        List<Pair> pairs = new ArrayList<>();
        for (Map.Entry<Integer, Integer> entry : piecesSentMap.entrySet()) {
            pairs.add(new Pair(entry.getKey(), entry.getValue()));
        }

        System.out.println(sdf.format(Calendar.getInstance().getTime()) + " - " + "SERVER: " + id + " before sorting pairs: ");
        for (Pair p : pairs)
            System.out.println(sdf.format(Calendar.getInstance().getTime()) + " - " + "nei: " + p.neiId + " : " + " freq: " + p.freq);
        Collections.sort(pairs, new Comparator<Pair>() {
            @Override
            public int compare(Pair p1, Pair p2) {
                return p1.freq - p2.freq;
            }
        });

        System.out.println(sdf.format(Calendar.getInstance().getTime()) + " - " + "SERVER: " + id + " after sorting pairs: ");
        for (Pair p : pairs)
            System.out.println("nei: " + p.neiId + " : " + " freq: " + p.freq);

        for (Pair p : pairs)
            neis.add(p.neiId);

        return neis;
    }
}


