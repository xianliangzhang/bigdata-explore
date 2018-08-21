package top.kou.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

import java.io.File;

public class AvroFileDemo {

    public static void main(String[] args) {
        Schema schema = AvroUtil.getSchema();


    }
}
