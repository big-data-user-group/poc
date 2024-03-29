package org.bd.poc;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class ImporterNewAPI {

    private static final Log LOG = LogFactory.getLog(ImporterNewAPI.class);

    public void initializeConfiguration(Configuration actionConf) throws IOException {
        JobConf conf = new JobConf(false);
        Job job = new Job(conf);
        DBInputFormat.setInput(job, DBRecord.class,
                "SELECT product_id, office_location_id, sold_price, date FROM sale", "SELECT COUNT(*) FROM sale");
        Configuration jdbcConfig = new Configuration();
        jdbcConfig.addResource("jdbc-default.xml");
        jdbcConfig.addResource("jdbc.xml");

        LOG.info(String.format("jdbc.properties driver: %s, url: %s, user: %s, password: %s",
                jdbcConfig.get("jdbc.driverClassName"),
                jdbcConfig.get("jdbc.url"),
                jdbcConfig.get("jdbc.username"),
                jdbcConfig.get("jdbc.password")
        ));
        DBConfiguration.configureDB(conf, jdbcConfig.get("jdbc.driverClassName"), jdbcConfig.get("jdbc.url"),
                jdbcConfig.get("jdbc.username"), jdbcConfig.get("jdbc.password"));

        job.setMapperClass(Map.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(0);

        for (java.util.Map.Entry<String, String> entry : conf) {
            LOG.info(String.format("%s: %s",  entry.getKey(), entry.getValue()));
            actionConf.set(entry.getKey(), entry.getValue());
        }
    }

    public static class Map extends Mapper<LongWritable, DBRecord, NullWritable, Text> {
        @Override
        protected void map(LongWritable key, DBRecord value, Context context) throws IOException, InterruptedException {
            List<String> fields = Lists.newArrayList(Long.toString(value.date), Long.toString(value.productID), Long.toString(value.locationID), value.totalPrice);
            String outLine = Joiner.on(",").join(fields);

            context.write(NullWritable.get(), new Text(outLine));
        }
    }

    static class DBRecord implements Writable, DBWritable {
        private long productID;
        private long locationID;
        private String totalPrice;
        private long date;

        public void readFields(DataInput in) throws IOException {
            this.productID = in.readLong();
            this.locationID = in.readLong();
            this.totalPrice = Text.readString(in);
            this.date = in.readLong();
        }

        public void readFields(ResultSet resultSet) throws SQLException {
            this.productID = resultSet.getLong(1);
            this.locationID = resultSet.getLong(2);
            this.totalPrice = resultSet.getBigDecimal(3).toString();
            this.date = resultSet.getDate(4).getTime();
        }

        public void write(DataOutput out) throws IOException {
            out.writeLong(this.productID);
            out.writeLong(this.locationID);
            Text.writeString(out, this.totalPrice);
            out.writeLong(this.date);
        }

        public void write(PreparedStatement stmt) throws SQLException {
            stmt.setLong(1, this.productID);
            stmt.setLong(2, this.locationID);
            stmt.setBigDecimal(3, new BigDecimal(this.totalPrice));
            stmt.setLong(4, this.date);
        }
    }
}
