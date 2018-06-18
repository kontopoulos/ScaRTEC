package integrated;

public class ComplexEvent {

    private String json;
    private long ingestionTimestamp;

    public ComplexEvent(String json, long ingestionTimestamp) {
        this.json = json;
        this.ingestionTimestamp = ingestionTimestamp;
    }

    public String getJson() {
        return json;
    }

    public long getIngestionTimestamp() {
        return ingestionTimestamp;
    }
}
