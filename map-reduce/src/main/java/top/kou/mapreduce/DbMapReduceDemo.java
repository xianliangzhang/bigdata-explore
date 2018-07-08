package top.kou.mapreduce;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapred.lib.db.DBInputFormat;
import org.apache.hadoop.mapred.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;

public class DbMapReduceDemo {

    static class RecordWritable implements Writable, DBWritable, WritableComparable<RecordWritable> {
        private LongWritable id;
        private Text name;
        private Text email;
        private Text address;

        public RecordWritable() {
            this.id = new LongWritable();
            this.name = new Text();
            this.email = new Text();
            this.address = new Text();
        }

        @Override
        public int compareTo(RecordWritable o) {
            return 0;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            id.write(dataOutput);
            name.write(dataOutput);
            email.write(dataOutput);
            address.write(dataOutput);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            id.readFields(dataInput);
            name.readFields(dataInput);
            email.readFields(dataInput);
            address.readFields(dataInput);
        }

        @Override
        public void write(PreparedStatement preparedStatement) throws SQLException {
            preparedStatement.setString(1, id.toString());
            preparedStatement.setString(2, name.toString());
            preparedStatement.setString(3, email.toString());
            preparedStatement.setString(4, address.toString());
        }

        @Override
        public void readFields(ResultSet resultSet) throws SQLException {
            id = new LongWritable(resultSet.getLong(1));
            name = new Text(resultSet.getString(2));
            email = new Text(resultSet.getString(3));
            address = new Text(resultSet.getString(4));
        }

        @Override
        public String toString() {
            return "RecordWritable{" +
                    "id=" + id +
                    ", name=" + name +
                    ", email=" + email +
                    ", address=" + address +
                    '}';
        }
    }

    static class DbRecordMapper extends MapReduceBase implements Mapper<LongWritable, RecordWritable, LongWritable, RecordWritable> {

        @Override
        public void map(LongWritable inputKey, RecordWritable inputValue, OutputCollector<LongWritable, RecordWritable> outputCollector, Reporter reporter) throws IOException {
            outputCollector.collect(inputValue.id, inputValue);
        }
    }

    static class DbRecordReducer extends MapReduceBase implements Reducer<LongWritable, RecordWritable, LongWritable, Text> {

        @Override
        public void reduce(LongWritable longWritable, Iterator<RecordWritable> iterator, OutputCollector<LongWritable, Text> outputCollector, Reporter reporter) throws IOException {
            while (iterator.hasNext()) {
                RecordWritable rw = iterator.next();
                outputCollector.collect(rw.id, new Text(rw.toString()));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf jobConf = new JobConf();

        jobConf.setMapperClass(DbRecordMapper.class);
        jobConf.setReducerClass(DbRecordReducer.class);

        jobConf.setMapOutputKeyClass(LongWritable.class);
        jobConf.setMapOutputValueClass(RecordWritable.class);

        String query = "select id, name, email, address from demo";
        String queryCount = "select count(*) from demo";
        DBConfiguration.configureDB(jobConf, "com.mysql.jdbc.Driver", "jdbc:mysql://10.111.16.154:8306/test", "root", "123456");
        DBInputFormat.setInput(jobConf, RecordWritable.class, query, queryCount);
        FileOutputFormat.setOutputPath(jobConf, new Path("output"));

        JobClient.runJob(jobConf).waitForCompletion();
    }
}
