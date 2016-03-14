/**
 * Created by Ash on 3/13/16.
 */
public class BitField {
    private int numOfPieces;
    private boolean[] bits;
    private int numOfPiecesFinished;
    private boolean isFinished;

    public BitField(int totalPieces) {
        this.numOfPieces = totalPieces;
        this.bits = new boolean[totalPieces];
        this.numOfPiecesFinished = 0;
        this.isFinished = false;
    }

    public void setBitToTrue(int i) {
        this.bits[i] = true;
        this.numOfPiecesFinished += 1;
        if (this.numOfPieces == this.numOfPiecesFinished) {
            this.isFinished = true;
        }
    }

    public void setBitsToTrue() {
        this.numOfPiecesFinished = this.numOfPieces;
        this.isFinished = true;
    }
}
