import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;

/**
 * Created by Ash on 3/13/16.
 */
public class Configuration {
    //For Common.cfg
    private int prefNeighboursCount;
    private int unchokingInterval;
    private int optUnchokingIntervl;
    private String fileName;
    private int fileSize;
    private int pieceSize;
    private int lastPieceSize;


    private int totalNumOfPieces;

    //For PeerInfo.cfg
    private ArrayList<Integer> peerID;
    private ArrayList<String> hostName;
    private ArrayList<Integer> portNumber;
    private ArrayList<Boolean> haveFile;

    private int totalPeers;

    public int getPrefNeighboursCount() {
        return prefNeighboursCount;
    }

    public int getUnchokingInterval() {
        return unchokingInterval;
    }

    public int getOptUnchokingIntervl() {
        return optUnchokingIntervl;
    }

    public String getFileName() {
        return fileName;
    }

    public int getFileSize() {
        return fileSize;
    }

    public int getPieceSize() {
        return pieceSize;
    }

    public int getLastPieceSize() {
        return lastPieceSize;
    }

    public int getTotalNumOfPieces() {
        return totalNumOfPieces;
    }

    public ArrayList<Integer> getPeerID() {
        return peerID;
    }

    public ArrayList<String> getHostName() {
        return hostName;
    }

    public ArrayList<Integer> getPortNumber() {
        return portNumber;
    }

    public ArrayList<Boolean> getHaveFile() {
        return haveFile;
    }

    public int getTotalPeers() {
        return totalPeers;
    }

    public Configuration(String commonConfigFile, String peerConfigFile) {
        Scanner scan;
        //For Common.cfg File
        FileReader fileReader;
        try {
            fileReader = new FileReader(commonConfigFile);

            scan = new Scanner(fileReader);
            try {
                String[] segments = scan.nextLine().split(" ");
                prefNeighboursCount = Integer.parseInt(segments[1].trim());
                segments = scan.nextLine().split(" ");
                unchokingInterval = Integer.parseInt(segments[1].trim());
                segments = scan.nextLine().split(" ");
                optUnchokingIntervl = Integer.parseInt(segments[1].trim());
                segments = scan.nextLine().split(" ");
                fileName = segments[1].trim();
                segments = scan.nextLine().split(" ");
                fileSize = Integer.parseInt(segments[1].trim());
                segments = scan.nextLine().split(" ");
                pieceSize = Integer.parseInt(segments[1].trim());
                this.totalNumOfPieces = (int) Math.ceil((double) fileSize / pieceSize);
                this.lastPieceSize = fileSize % pieceSize;
                System.out.println("##### Configuration  : The total number of chunks are: " + this.totalNumOfPieces);
            } finally {
                scan.close();
                fileReader.close();
            }
        } catch (FileNotFoundException ex) {
            System.out.println(ex.toString());
        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        }

        try {
            //For peerConfig.cfg file
            fileReader = new FileReader(peerConfigFile);

            peerID = new ArrayList<Integer>();
            hostName = new ArrayList<String>();
            portNumber = new ArrayList<Integer>();
            haveFile = new ArrayList<Boolean>();

            scan = new Scanner(fileReader);
            try {
                int numberOfPeers = 0;
                while (scan.hasNextLine()) {
                    numberOfPeers += 1;
                    String[] segments = scan.nextLine().split(" ");
                    peerID.add(Integer.parseInt(segments[0].trim()));
                    hostName.add(segments[1].trim());
                    portNumber.add(Integer.parseInt(segments[2].trim()));
                    if (Integer.parseInt(segments[3].trim()) == 1) {
                        haveFile.add(true);
                    } else {
                        haveFile.add(false);
                    }
                }
                this.totalPeers = numberOfPeers;
            } finally {
                scan.close();
                fileReader.close();
            }
        } catch (FileNotFoundException ex) {
            System.out.println(ex.getMessage());
        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        }
    }
}
