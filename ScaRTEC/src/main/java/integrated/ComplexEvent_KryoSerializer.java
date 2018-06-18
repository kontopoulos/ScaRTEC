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

public class ComplexEvent_KryoSerializer  implements Closeable, AutoCloseable, Serializer<ComplexEvent>, Deserializer<ComplexEvent> {

    private ThreadLocal<Kryo> kryos = new ThreadLocal<Kryo>() {
        protected Kryo initialValue() {
            Kryo kryo = new Kryo();
            kryo.addDefaultSerializer(ComplexEvent.class, new ComplexEvent_KryoSerializer.KryoInternalSerializer());
            return kryo;
        }
    };

    @Override
    public ComplexEvent deserialize(String s, byte[] bytes) {
        try {
            return kryos.get().readObject(new ByteBufferInput(bytes), ComplexEvent.class);
        }
        catch(Exception e) {
            throw new IllegalArgumentException("Error reading bytes",e);
        }
    }

    @Override
    public void configure(Map<String, ?> map, boolean b) {
    }

    @Override
    public byte[] serialize(String s, ComplexEvent complexEvent) {
        ByteBufferOutput output = new ByteBufferOutput(40960);
        kryos.get().writeObject(output, complexEvent);
        return output.toBytes();
    }

    @Override
    public void close() {

    }

    private static class KryoInternalSerializer extends com.esotericsoftware.kryo.Serializer<ComplexEvent> {
        @Override
        public void write(Kryo kryo, Output output, ComplexEvent customer) {
            output.writeString(customer.getJson());
            output.writeLong(customer.getIngestionTimestamp(),true);
        }

        @Override
        public ComplexEvent read(Kryo kryo, Input input, Class<ComplexEvent> aClass) {
            String json = input.readString();
            long ingestionTimestamp = input.readLong(true);

            return new ComplexEvent(json,ingestionTimestamp);
        }
    }
}
