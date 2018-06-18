package integrated;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferInput;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.Closeable;
import java.util.Map;

/**
 * Created by aglenis on 4/27/17.
 */

public class ST_RDF_KryoSerializer implements Closeable, AutoCloseable, Serializer<ST_RDF>, Deserializer<ST_RDF> {


    private ThreadLocal<Kryo> kryos = new ThreadLocal<Kryo>() {
        protected Kryo initialValue() {
            Kryo kryo = new Kryo();
            kryo.addDefaultSerializer(ST_RDF.class, new KryoInternalSerializer());
            return kryo;
        }
    };

    @Override
    public void configure(Map<String, ?> map, boolean b) {
    }

    @Override
    public byte[] serialize(String s, ST_RDF customer) {
        ByteBufferOutput output = new ByteBufferOutput(40960);
        kryos.get().writeObject(output, customer);
        return output.toBytes();
    }

    @Override
    public ST_RDF deserialize(String s, byte[] bytes) {
        try {
            return kryos.get().readObject(new ByteBufferInput(bytes), ST_RDF.class);
        }
        catch(Exception e) {
            throw new IllegalArgumentException("Error reading bytes",e);
        }
    }

    @Override
    public void close() {

    }

    private static class KryoInternalSerializer extends com.esotericsoftware.kryo.Serializer<ST_RDF> {
        @Override
        public void write(Kryo kryo, Output output, ST_RDF customer) {
            output.writeBoolean(customer.hasSpatiotemporal);
            output.writeString(customer.rdfPart);
            output.writeDouble(customer.latitude);
            output.writeDouble(customer.longitude);
            output.writeDouble(customer.height1);
            output.writeLong(customer.time1,true);
            output.writeString(customer.idToEncode);
            output.writeLong(customer.ingestionTimestamp,true);
        }

        @Override
        public ST_RDF read(Kryo kryo, Input input, Class<ST_RDF> aClass) {
            boolean hasSpatiotemporal = input.readBoolean();
            String rdfPart = input.readString();
            double latitude = input.readDouble();
            double longitude = input.readDouble();
            double height1 = input.readDouble();
            long time1 = input.readLong(true);
            String idToEncode = input.readString();
            long ingestionTimestamp = input.readLong(true);

            return new ST_RDF(hasSpatiotemporal,rdfPart,latitude,longitude,height1,time1,idToEncode,ingestionTimestamp);
        }
    }
}