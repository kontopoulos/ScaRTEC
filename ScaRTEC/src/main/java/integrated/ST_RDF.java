package integrated;

/**
 * Created by ikon on 14/7/2017.
 */
public class ST_RDF {
    public boolean hasSpatiotemporal;
    public String rdfPart;
    public double latitude;
    public double longitude;
    public double height1;
    public long time1;
    public String idToEncode; //all those could be a list

    public ST_RDF(boolean hasSpatiotemporal, String rdfPart, double latitude, double longitude, double height1, long time1,String idToEncode) {
        this.hasSpatiotemporal = hasSpatiotemporal;
        this.rdfPart = rdfPart;
        this.latitude = latitude;
        this.longitude = longitude;
        this.height1 = height1;
        this.time1 = time1;
        this.idToEncode = idToEncode;
    }
}
