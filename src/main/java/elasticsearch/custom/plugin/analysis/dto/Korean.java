package elasticsearch.custom.plugin.analysis.dto;

public class Korean {
    private boolean isCombinedKorean = false;
    private char choSeong;
    private char jungSeong;
    private char jongSeong;
    private char otherChar;
    private int idxChoSeong;    // 0 ~ 18
    private int idxJungSeong;   // 0 ~ 20
    private int idxJongSeong;   // 0 ~ 27

    public boolean isCombinedKorean() {
        return isCombinedKorean;
    }

    public void setCombinedKorean() {
        isCombinedKorean = true;
    }

    public char getChoSeong() {
        return choSeong;
    }

    public void setChoSeong(char choSeong) {
        this.choSeong = choSeong;
    }

    public char getJungSeong() {
        return jungSeong;
    }

    public void setJungSeong(char jungSeong) {
        this.jungSeong = jungSeong;
    }

    public char getJongSeong() {
        return jongSeong;
    }

    public void setJongSeong(char jongSeong) {
        this.jongSeong = jongSeong;
    }

    public char getOtherChar() {
        return otherChar;
    }

    public void setOtherChar(char otherChar) {
        this.otherChar = otherChar;
    }

    public int getIdxChoSeong() {
        return idxChoSeong;
    }

    public void setIdxChoSeong(int idxChoSeong) {
        this.idxChoSeong = idxChoSeong;
    }

    public int getIdxJungSeong() {
        return idxJungSeong;
    }

    public void setIdxJungSeong(int idxJungSeong) {
        this.idxJungSeong = idxJungSeong;
    }

    public int getIdxJongSeong() {
        return idxJongSeong;
    }

    public void setIdxJongSeong(int idxJongSeong) {
        this.idxJongSeong = idxJongSeong;
    }
}
