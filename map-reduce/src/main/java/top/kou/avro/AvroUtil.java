package top.kou.avro;

import org.apache.avro.Schema;

import java.io.IOException;

public class AvroUtil {

    public static final Schema getSchema() {
        try {
            Schema.Parser parser = new Schema.Parser();
            System.out.println(AvroDemo.class.getResource("/StringPair.avsc"));
            Schema schema = parser.parse(AvroDemo.class.getResourceAsStream("/StringPair.avsc"));
            System.out.println(schema);
            return schema;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
