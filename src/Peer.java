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


public class Peer implements Runnable{

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

    public Peer(int id){
        config = new Configuration("Common.cfg", "PeerInfo.cfg");

        this.id = id;
        N = config.getTotalPeers();
        K = config.getPrefNeighboursCount();
        p = config.getUnchokingInterval();
        m = config.getOptUnchokingIntervl();
        noOfPieces = config.getTotalNumOfPieces();

        peerids = new int[N];
        bitfieldsMap = new HashMap<>();
        handShakeMap = new HashMap<>();

        peerLog = new P2PLogger(this.id);
    }

    @Override
    public void run(){

        System.out.println("Started running peer: " + id);

        List<String> hosts = config.getHostName();
        List<Integer> ports = config.getPortNumber();
        List<Integer> ids = config.getPeerID();
        for(int i=0; i<N; i++) {
            peerids[i] = ids.get(i);
            if(peerids[i] == id){
                idx = i;
            }
        }

        if(config.getHaveFile().get(idx)){
            BitSet bitSet = new BitSet();
            bitSet.set(0, noOfPieces);
            bitfieldsMap.put(id, bitSet);
        }else{
            bitfieldsMap.put(id, new BitSet());
        }

        System.out.println("BitSet: " + bitfieldsMap.get(id));

        ExecutorService clientExecutorService = Executors.newFixedThreadPool(N);
        for (int i = 0; i < N; i++) {
            if(i == idx) break;
            try{
                Socket clientSocket = new Socket(hosts.get(i), ports.get(i));
                System.out.println("connecting to server: " + hosts.get(i) + " port: " + ports.get(i));
                clientExecutorService.submit(new ClientHandler(clientSocket, id));
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        clientExecutorService.shutdown();

        ExecutorService serverExecutorService = Executors.newFixedThreadPool(N);
        try{
            ServerSocket serverSocket = new ServerSocket(ports.get(idx));
            //client TCPs count
            int TCPcount = 0;
            while(TCPcount < N - 1 - idx){  //@TODO change logic
                serverExecutorService.submit(new ServerHandler(serverSocket.accept()));
                TCPcount++;
                System.out.println("server side TCP count: " + TCPcount);
            }
        }catch (Exception e) {
            e.printStackTrace();
        }
        serverExecutorService.shutdown();
        System.out.println("shutting down Peer services " + id);
    }

    public static void main(String[] args) throws Exception{
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
            try{
                System.out.println("closing server socket at port " + socket.getLocalPort());
                socket.close();
            }catch (Exception e){
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

            } catch (Exception e) {
                e.printStackTrace();
                return 0;
            }
            return 1;
        }
    }

    class ClientHandler implements Callable<String>{

        private Socket socket;
        private int serverId;

        ClientHandler(Socket socket, int serverId){
            this.socket = socket;
            this.serverId = serverId;
        }

        @Override
        public String call() {
            int status = client_communication();
            try{
                System.out.println("closing client socket at port " + socket.getLocalPort());
                socket.close();
            }catch (Exception e){
                e.printStackTrace();
            }
            return "status: " + status;
        }

        //return status
        // 1 - success
        // 0 - error
        private int client_communication(){

            try(
                    OutputStream out = socket.getOutputStream();
                    InputStream in = socket.getInputStream();

            ){
                    /*
                        1. Handshake send to server
                     */
                    Handshake handshake = new Handshake();
                    handshake.setPeerId(id);
                    handshake.sendHandshakeMsg(socket);
                    handShakeMap.put(serverId, false);
                    handshake.handShakeReceived(socket);
                    System.out.println("Handshake initiation received from " + serverId);
                    if (handShakeMap.get(serverId) != null) {
                        handShakeMap.put(serverId, true);
                        peerLog.logHandshakeSuccess(serverId, Calendar.getInstance());
                        System.out.println("Handshake Success");
                    }
                    peerLog.logTcpConnection(serverId, Calendar.getInstance());

                    /*
                        2. bitfield send to server
                     */

            }catch(Exception e){
                e.printStackTrace();
                return 0;
            }
            return 1;
        }
    }
}


