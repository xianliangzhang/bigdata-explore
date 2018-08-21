package top.kou.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

public class AvroDemo {
    public static void main(String[] args) throws IOException {
        Schema.Parser parser = new Schema.Parser();
        System.out.println(AvroDemo.class.getResource("/StringPair.avsc"));
        Schema schema = parser.parse(AvroDemo.class.getResourceAsStream("/StringPair.avsc"));
        System.out.println(schema);

        GenericRecord datum = new GenericData.Record(schema);
        datum.put("left", "L");
        datum.put("right", "R");

        // 内存操作
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        datumWriter.write(datum, encoder);
        encoder.flush();
        out.close();

        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        Decoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
        GenericRecord record = datumReader.read(null, decoder);
        assert record.get("left").equals("L");
        assert record.get("right").equals("R");


        // 文件操作
        File file = new File("data.avro");
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(writer);
        dataFileWriter.create(schema, file);
        dataFileWriter.append(datum);
        dataFileWriter.close();

        DatumReader<GenericRecord> reader = new GenericDatumReader<>();
        DataFileReader<GenericRecord> fileReader = new DataFileReader<GenericRecord>(file, reader);
        assert fileReader.getSchema().equals(schema);
        GenericRecord gr = null;
        while (fileReader.hasNext()) {
            gr = fileReader.next();
            System.out.println(" --" + gr.get("left") + " -- " + gr.get("right"));
        }


        System.out.println("OK");
    }
}
