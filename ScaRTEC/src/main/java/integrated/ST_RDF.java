package integrated;

import org.apache.avro.specific.SpecificData;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.SchemaStore;

@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class ST_RDF extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
    private static final long serialVersionUID = 2608010606812249445L;
    public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"ST_RDF\",\"fields\":[{\"name\":\"hasSpatiotemporal\",\"type\":\"boolean\"},{\"name\":\"rdfPart\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},{\"name\":\"latitude\",\"type\":\"double\"},{\"name\":\"longitude\",\"type\":\"double\"},{\"name\":\"height1\",\"type\":\"double\"},{\"name\":\"time1\",\"type\":\"long\"},{\"name\":\"idToEncode\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},{\"name\":\"ingestionTimestamp\",\"type\":\"long\"}]}");
    public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

    private static SpecificData MODEL$ = new SpecificData();

    private static final BinaryMessageEncoder<ST_RDF> ENCODER =
            new BinaryMessageEncoder<ST_RDF>(MODEL$, SCHEMA$);

    private static final BinaryMessageDecoder<ST_RDF> DECODER =
            new BinaryMessageDecoder<ST_RDF>(MODEL$, SCHEMA$);

    /**
     * Return the BinaryMessageDecoder instance used by this class.
     */
    public static BinaryMessageDecoder<ST_RDF> getDecoder() {
        return DECODER;
    }

    /**
     * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link SchemaStore}.
     * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
     */
    public static BinaryMessageDecoder<ST_RDF> createDecoder(SchemaStore resolver) {
        return new BinaryMessageDecoder<ST_RDF>(MODEL$, SCHEMA$, resolver);
    }

    /** Serializes this ST_RDF to a ByteBuffer. */
    public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
        return ENCODER.encode(this);
    }

    /** Deserializes a ST_RDF from a ByteBuffer. */
    public static ST_RDF fromByteBuffer(
            java.nio.ByteBuffer b) throws java.io.IOException {
        return DECODER.decode(b);
    }

    @Deprecated public boolean hasSpatiotemporal;
    @Deprecated public java.lang.String rdfPart;
    @Deprecated public double latitude;
    @Deprecated public double longitude;
    @Deprecated public double height1;
    @Deprecated public long time1;
    @Deprecated public java.lang.String idToEncode;
    @Deprecated public long ingestionTimestamp;

    /**
     * Default constructor.  Note that this does not initialize fields
     * to their default values from the schema.  If that is desired then
     * one should use <code>newBuilder()</code>.
     */
    public ST_RDF() {}

    /**
     * All-args constructor.
     * @param hasSpatiotemporal The new value for hasSpatiotemporal
     * @param rdfPart The new value for rdfPart
     * @param latitude The new value for latitude
     * @param longitude The new value for longitude
     * @param height1 The new value for height1
     * @param time1 The new value for time1
     * @param idToEncode The new value for idToEncode
     * @param ingestionTimestamp The new value for ingestionTimestamp
     */
    public ST_RDF(java.lang.Boolean hasSpatiotemporal, java.lang.String rdfPart, java.lang.Double latitude, java.lang.Double longitude, java.lang.Double height1, java.lang.Long time1, java.lang.String idToEncode, java.lang.Long ingestionTimestamp) {
        this.hasSpatiotemporal = hasSpatiotemporal;
        this.rdfPart = rdfPart;
        this.latitude = latitude;
        this.longitude = longitude;
        this.height1 = height1;
        this.time1 = time1;
        this.idToEncode = idToEncode;
        this.ingestionTimestamp = ingestionTimestamp;
    }

    public org.apache.avro.Schema getSchema() { return SCHEMA$; }
    // Used by DatumWriter.  Applications should not call.
    public java.lang.Object get(int field$) {
        switch (field$) {
            case 0: return hasSpatiotemporal;
            case 1: return rdfPart;
            case 2: return latitude;
            case 3: return longitude;
            case 4: return height1;
            case 5: return time1;
            case 6: return idToEncode;
            case 7: return ingestionTimestamp;
            default: throw new org.apache.avro.AvroRuntimeException("Bad index");
        }
    }

    // Used by DatumReader.  Applications should not call.
    @SuppressWarnings(value="unchecked")
    public void put(int field$, java.lang.Object value$) {
        switch (field$) {
            case 0: hasSpatiotemporal = (java.lang.Boolean)value$; break;
            case 1: rdfPart = (java.lang.String)value$; break;
            case 2: latitude = (java.lang.Double)value$; break;
            case 3: longitude = (java.lang.Double)value$; break;
            case 4: height1 = (java.lang.Double)value$; break;
            case 5: time1 = (java.lang.Long)value$; break;
            case 6: idToEncode = (java.lang.String)value$; break;
            case 7: ingestionTimestamp = (java.lang.Long)value$; break;
            default: throw new org.apache.avro.AvroRuntimeException("Bad index");
        }
    }

    /**
     * Gets the value of the 'hasSpatiotemporal' field.
     * @return The value of the 'hasSpatiotemporal' field.
     */
    public java.lang.Boolean getHasSpatiotemporal() {
        return hasSpatiotemporal;
    }

    /**
     * Sets the value of the 'hasSpatiotemporal' field.
     * @param value the value to set.
     */
    public void setHasSpatiotemporal(java.lang.Boolean value) {
        this.hasSpatiotemporal = value;
    }

    /**
     * Gets the value of the 'rdfPart' field.
     * @return The value of the 'rdfPart' field.
     */
    public java.lang.String getRdfPart() {
        return rdfPart;
    }

    /**
     * Sets the value of the 'rdfPart' field.
     * @param value the value to set.
     */
    public void setRdfPart(java.lang.String value) {
        this.rdfPart = value;
    }

    /**
     * Gets the value of the 'latitude' field.
     * @return The value of the 'latitude' field.
     */
    public java.lang.Double getLatitude() {
        return latitude;
    }

    /**
     * Sets the value of the 'latitude' field.
     * @param value the value to set.
     */
    public void setLatitude(java.lang.Double value) {
        this.latitude = value;
    }

    /**
     * Gets the value of the 'longitude' field.
     * @return The value of the 'longitude' field.
     */
    public java.lang.Double getLongitude() {
        return longitude;
    }

    /**
     * Sets the value of the 'longitude' field.
     * @param value the value to set.
     */
    public void setLongitude(java.lang.Double value) {
        this.longitude = value;
    }

    /**
     * Gets the value of the 'height1' field.
     * @return The value of the 'height1' field.
     */
    public java.lang.Double getHeight1() {
        return height1;
    }

    /**
     * Sets the value of the 'height1' field.
     * @param value the value to set.
     */
    public void setHeight1(java.lang.Double value) {
        this.height1 = value;
    }

    /**
     * Gets the value of the 'time1' field.
     * @return The value of the 'time1' field.
     */
    public java.lang.Long getTime1() {
        return time1;
    }

    /**
     * Sets the value of the 'time1' field.
     * @param value the value to set.
     */
    public void setTime1(java.lang.Long value) {
        this.time1 = value;
    }

    /**
     * Gets the value of the 'idToEncode' field.
     * @return The value of the 'idToEncode' field.
     */
    public java.lang.String getIdToEncode() {
        return idToEncode;
    }

    /**
     * Sets the value of the 'idToEncode' field.
     * @param value the value to set.
     */
    public void setIdToEncode(java.lang.String value) {
        this.idToEncode = value;
    }

    /**
     * Gets the value of the 'ingestionTimestamp' field.
     * @return The value of the 'ingestionTimestamp' field.
     */
    public java.lang.Long getIngestionTimestamp() {
        return ingestionTimestamp;
    }

    /**
     * Sets the value of the 'ingestionTimestamp' field.
     * @param value the value to set.
     */
    public void setIngestionTimestamp(java.lang.Long value) {
        this.ingestionTimestamp = value;
    }

    /**
     * Creates a new ST_RDF RecordBuilder.
     * @return A new ST_RDF RecordBuilder
     */
    public static ST_RDF.Builder newBuilder() {
        return new ST_RDF.Builder();
    }

    /**
     * Creates a new ST_RDF RecordBuilder by copying an existing Builder.
     * @param other The existing builder to copy.
     * @return A new ST_RDF RecordBuilder
     */
    public static ST_RDF.Builder newBuilder(ST_RDF.Builder other) {
        return new ST_RDF.Builder(other);
    }

    /**
     * Creates a new ST_RDF RecordBuilder by copying an existing ST_RDF instance.
     * @param other The existing instance to copy.
     * @return A new ST_RDF RecordBuilder
     */
    public static ST_RDF.Builder newBuilder(ST_RDF other) {
        return new ST_RDF.Builder(other);
    }

    /**
     * RecordBuilder for ST_RDF instances.
     */
    public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<ST_RDF>
            implements org.apache.avro.data.RecordBuilder<ST_RDF> {

        private boolean hasSpatiotemporal;
        private java.lang.String rdfPart;
        private double latitude;
        private double longitude;
        private double height1;
        private long time1;
        private java.lang.String idToEncode;
        private long ingestionTimestamp;

        /** Creates a new Builder */
        private Builder() {
            super(SCHEMA$);
        }

        /**
         * Creates a Builder by copying an existing Builder.
         * @param other The existing Builder to copy.
         */
        private Builder(ST_RDF.Builder other) {
            super(other);
            if (isValidValue(fields()[0], other.hasSpatiotemporal)) {
                this.hasSpatiotemporal = data().deepCopy(fields()[0].schema(), other.hasSpatiotemporal);
                fieldSetFlags()[0] = true;
            }
            if (isValidValue(fields()[1], other.rdfPart)) {
                this.rdfPart = data().deepCopy(fields()[1].schema(), other.rdfPart);
                fieldSetFlags()[1] = true;
            }
            if (isValidValue(fields()[2], other.latitude)) {
                this.latitude = data().deepCopy(fields()[2].schema(), other.latitude);
                fieldSetFlags()[2] = true;
            }
            if (isValidValue(fields()[3], other.longitude)) {
                this.longitude = data().deepCopy(fields()[3].schema(), other.longitude);
                fieldSetFlags()[3] = true;
            }
            if (isValidValue(fields()[4], other.height1)) {
                this.height1 = data().deepCopy(fields()[4].schema(), other.height1);
                fieldSetFlags()[4] = true;
            }
            if (isValidValue(fields()[5], other.time1)) {
                this.time1 = data().deepCopy(fields()[5].schema(), other.time1);
                fieldSetFlags()[5] = true;
            }
            if (isValidValue(fields()[6], other.idToEncode)) {
                this.idToEncode = data().deepCopy(fields()[6].schema(), other.idToEncode);
                fieldSetFlags()[6] = true;
            }
            if (isValidValue(fields()[7], other.ingestionTimestamp)) {
                this.ingestionTimestamp = data().deepCopy(fields()[7].schema(), other.ingestionTimestamp);
                fieldSetFlags()[7] = true;
            }
        }

        /**
         * Creates a Builder by copying an existing ST_RDF instance
         * @param other The existing instance to copy.
         */
        private Builder(ST_RDF other) {
            super(SCHEMA$);
            if (isValidValue(fields()[0], other.hasSpatiotemporal)) {
                this.hasSpatiotemporal = data().deepCopy(fields()[0].schema(), other.hasSpatiotemporal);
                fieldSetFlags()[0] = true;
            }
            if (isValidValue(fields()[1], other.rdfPart)) {
                this.rdfPart = data().deepCopy(fields()[1].schema(), other.rdfPart);
                fieldSetFlags()[1] = true;
            }
            if (isValidValue(fields()[2], other.latitude)) {
                this.latitude = data().deepCopy(fields()[2].schema(), other.latitude);
                fieldSetFlags()[2] = true;
            }
            if (isValidValue(fields()[3], other.longitude)) {
                this.longitude = data().deepCopy(fields()[3].schema(), other.longitude);
                fieldSetFlags()[3] = true;
            }
            if (isValidValue(fields()[4], other.height1)) {
                this.height1 = data().deepCopy(fields()[4].schema(), other.height1);
                fieldSetFlags()[4] = true;
            }
            if (isValidValue(fields()[5], other.time1)) {
                this.time1 = data().deepCopy(fields()[5].schema(), other.time1);
                fieldSetFlags()[5] = true;
            }
            if (isValidValue(fields()[6], other.idToEncode)) {
                this.idToEncode = data().deepCopy(fields()[6].schema(), other.idToEncode);
                fieldSetFlags()[6] = true;
            }
            if (isValidValue(fields()[7], other.ingestionTimestamp)) {
                this.ingestionTimestamp = data().deepCopy(fields()[7].schema(), other.ingestionTimestamp);
                fieldSetFlags()[7] = true;
            }
        }

        /**
         * Gets the value of the 'hasSpatiotemporal' field.
         * @return The value.
         */
        public java.lang.Boolean getHasSpatiotemporal() {
            return hasSpatiotemporal;
        }

        /**
         * Sets the value of the 'hasSpatiotemporal' field.
         * @param value The value of 'hasSpatiotemporal'.
         * @return This builder.
         */
        public ST_RDF.Builder setHasSpatiotemporal(boolean value) {
            validate(fields()[0], value);
            this.hasSpatiotemporal = value;
            fieldSetFlags()[0] = true;
            return this;
        }

        /**
         * Checks whether the 'hasSpatiotemporal' field has been set.
         * @return True if the 'hasSpatiotemporal' field has been set, false otherwise.
         */
        public boolean hasHasSpatiotemporal() {
            return fieldSetFlags()[0];
        }


        /**
         * Clears the value of the 'hasSpatiotemporal' field.
         * @return This builder.
         */
        public ST_RDF.Builder clearHasSpatiotemporal() {
            fieldSetFlags()[0] = false;
            return this;
        }

        /**
         * Gets the value of the 'rdfPart' field.
         * @return The value.
         */
        public java.lang.String getRdfPart() {
            return rdfPart;
        }

        /**
         * Sets the value of the 'rdfPart' field.
         * @param value The value of 'rdfPart'.
         * @return This builder.
         */
        public ST_RDF.Builder setRdfPart(java.lang.String value) {
            validate(fields()[1], value);
            this.rdfPart = value;
            fieldSetFlags()[1] = true;
            return this;
        }

        /**
         * Checks whether the 'rdfPart' field has been set.
         * @return True if the 'rdfPart' field has been set, false otherwise.
         */
        public boolean hasRdfPart() {
            return fieldSetFlags()[1];
        }


        /**
         * Clears the value of the 'rdfPart' field.
         * @return This builder.
         */
        public ST_RDF.Builder clearRdfPart() {
            rdfPart = null;
            fieldSetFlags()[1] = false;
            return this;
        }

        /**
         * Gets the value of the 'latitude' field.
         * @return The value.
         */
        public java.lang.Double getLatitude() {
            return latitude;
        }

        /**
         * Sets the value of the 'latitude' field.
         * @param value The value of 'latitude'.
         * @return This builder.
         */
        public ST_RDF.Builder setLatitude(double value) {
            validate(fields()[2], value);
            this.latitude = value;
            fieldSetFlags()[2] = true;
            return this;
        }

        /**
         * Checks whether the 'latitude' field has been set.
         * @return True if the 'latitude' field has been set, false otherwise.
         */
        public boolean hasLatitude() {
            return fieldSetFlags()[2];
        }


        /**
         * Clears the value of the 'latitude' field.
         * @return This builder.
         */
        public ST_RDF.Builder clearLatitude() {
            fieldSetFlags()[2] = false;
            return this;
        }

        /**
         * Gets the value of the 'longitude' field.
         * @return The value.
         */
        public java.lang.Double getLongitude() {
            return longitude;
        }

        /**
         * Sets the value of the 'longitude' field.
         * @param value The value of 'longitude'.
         * @return This builder.
         */
        public ST_RDF.Builder setLongitude(double value) {
            validate(fields()[3], value);
            this.longitude = value;
            fieldSetFlags()[3] = true;
            return this;
        }

        /**
         * Checks whether the 'longitude' field has been set.
         * @return True if the 'longitude' field has been set, false otherwise.
         */
        public boolean hasLongitude() {
            return fieldSetFlags()[3];
        }


        /**
         * Clears the value of the 'longitude' field.
         * @return This builder.
         */
        public ST_RDF.Builder clearLongitude() {
            fieldSetFlags()[3] = false;
            return this;
        }

        /**
         * Gets the value of the 'height1' field.
         * @return The value.
         */
        public java.lang.Double getHeight1() {
            return height1;
        }

        /**
         * Sets the value of the 'height1' field.
         * @param value The value of 'height1'.
         * @return This builder.
         */
        public ST_RDF.Builder setHeight1(double value) {
            validate(fields()[4], value);
            this.height1 = value;
            fieldSetFlags()[4] = true;
            return this;
        }

        /**
         * Checks whether the 'height1' field has been set.
         * @return True if the 'height1' field has been set, false otherwise.
         */
        public boolean hasHeight1() {
            return fieldSetFlags()[4];
        }


        /**
         * Clears the value of the 'height1' field.
         * @return This builder.
         */
        public ST_RDF.Builder clearHeight1() {
            fieldSetFlags()[4] = false;
            return this;
        }

        /**
         * Gets the value of the 'time1' field.
         * @return The value.
         */
        public java.lang.Long getTime1() {
            return time1;
        }

        /**
         * Sets the value of the 'time1' field.
         * @param value The value of 'time1'.
         * @return This builder.
         */
        public ST_RDF.Builder setTime1(long value) {
            validate(fields()[5], value);
            this.time1 = value;
            fieldSetFlags()[5] = true;
            return this;
        }

        /**
         * Checks whether the 'time1' field has been set.
         * @return True if the 'time1' field has been set, false otherwise.
         */
        public boolean hasTime1() {
            return fieldSetFlags()[5];
        }


        /**
         * Clears the value of the 'time1' field.
         * @return This builder.
         */
        public ST_RDF.Builder clearTime1() {
            fieldSetFlags()[5] = false;
            return this;
        }

        /**
         * Gets the value of the 'idToEncode' field.
         * @return The value.
         */
        public java.lang.String getIdToEncode() {
            return idToEncode;
        }

        /**
         * Sets the value of the 'idToEncode' field.
         * @param value The value of 'idToEncode'.
         * @return This builder.
         */
        public ST_RDF.Builder setIdToEncode(java.lang.String value) {
            validate(fields()[6], value);
            this.idToEncode = value;
            fieldSetFlags()[6] = true;
            return this;
        }

        /**
         * Checks whether the 'idToEncode' field has been set.
         * @return True if the 'idToEncode' field has been set, false otherwise.
         */
        public boolean hasIdToEncode() {
            return fieldSetFlags()[6];
        }


        /**
         * Clears the value of the 'idToEncode' field.
         * @return This builder.
         */
        public ST_RDF.Builder clearIdToEncode() {
            idToEncode = null;
            fieldSetFlags()[6] = false;
            return this;
        }

        /**
         * Gets the value of the 'ingestionTimestamp' field.
         * @return The value.
         */
        public java.lang.Long getIngestionTimestamp() {
            return ingestionTimestamp;
        }

        /**
         * Sets the value of the 'ingestionTimestamp' field.
         * @param value The value of 'ingestionTimestamp'.
         * @return This builder.
         */
        public ST_RDF.Builder setIngestionTimestamp(long value) {
            validate(fields()[7], value);
            this.ingestionTimestamp = value;
            fieldSetFlags()[7] = true;
            return this;
        }

        /**
         * Checks whether the 'ingestionTimestamp' field has been set.
         * @return True if the 'ingestionTimestamp' field has been set, false otherwise.
         */
        public boolean hasIngestionTimestamp() {
            return fieldSetFlags()[7];
        }


        /**
         * Clears the value of the 'ingestionTimestamp' field.
         * @return This builder.
         */
        public ST_RDF.Builder clearIngestionTimestamp() {
            fieldSetFlags()[7] = false;
            return this;
        }

        @Override
        @SuppressWarnings("unchecked")
        public ST_RDF build() {
            try {
                ST_RDF record = new ST_RDF();
                record.hasSpatiotemporal = fieldSetFlags()[0] ? this.hasSpatiotemporal : (java.lang.Boolean) defaultValue(fields()[0]);
                record.rdfPart = fieldSetFlags()[1] ? this.rdfPart : (java.lang.String) defaultValue(fields()[1]);
                record.latitude = fieldSetFlags()[2] ? this.latitude : (java.lang.Double) defaultValue(fields()[2]);
                record.longitude = fieldSetFlags()[3] ? this.longitude : (java.lang.Double) defaultValue(fields()[3]);
                record.height1 = fieldSetFlags()[4] ? this.height1 : (java.lang.Double) defaultValue(fields()[4]);
                record.time1 = fieldSetFlags()[5] ? this.time1 : (java.lang.Long) defaultValue(fields()[5]);
                record.idToEncode = fieldSetFlags()[6] ? this.idToEncode : (java.lang.String) defaultValue(fields()[6]);
                record.ingestionTimestamp = fieldSetFlags()[7] ? this.ingestionTimestamp : (java.lang.Long) defaultValue(fields()[7]);
                return record;
            } catch (java.lang.Exception e) {
                throw new org.apache.avro.AvroRuntimeException(e);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static final org.apache.avro.io.DatumWriter<ST_RDF>
            WRITER$ = (org.apache.avro.io.DatumWriter<ST_RDF>)MODEL$.createDatumWriter(SCHEMA$);

    @Override public void writeExternal(java.io.ObjectOutput out)
            throws java.io.IOException {
        WRITER$.write(this, SpecificData.getEncoder(out));
    }

    @SuppressWarnings("unchecked")
    private static final org.apache.avro.io.DatumReader<ST_RDF>
            READER$ = (org.apache.avro.io.DatumReader<ST_RDF>)MODEL$.createDatumReader(SCHEMA$);

    @Override public void readExternal(java.io.ObjectInput in)
            throws java.io.IOException {
        READER$.read(this, SpecificData.getDecoder(in));
    }

}