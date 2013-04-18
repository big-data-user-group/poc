package org.bd.poc;

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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ImporterNewAPIAvro {

    private static final Log LOG = LogFactory.getLog(ImporterNewAPIAvro.class);

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

        AvroJob.setMapOutputKeySchema(job, Sale.SCHEMA$);
        AvroJob.setOutputKeySchema(job, Sale.SCHEMA$);
        job.setOutputValueClass(NullWritable.class);

        job.setMapperClass(Map.class);

        job.setOutputFormatClass(AvroKeyOutputFormat.class);

        job.setNumReduceTasks(0);

        for (java.util.Map.Entry<String, String> entry : conf) {
            if (!entry.getKey().equalsIgnoreCase("io.serializations")) {
                LOG.info(String.format("%s: %s",  entry.getKey(), entry.getValue()));
                actionConf.set(entry.getKey(), entry.getValue());
            } else {
                AvroSerialization.addToConfiguration(actionConf);
            }
        }
    }

    public static class Map extends Mapper<LongWritable, DBRecord, AvroKey<Sale>, NullWritable> {
        @Override
        protected void map(LongWritable key, DBRecord value, Context context) throws IOException, InterruptedException {
            Sale sale = Sale.newBuilder()
                    .setProductId(Long.toString(value.productID))
                    .setOfficeLocationId(Long.toString(value.locationID))
                    .setSoldPrice(value.totalPrice)
                    .setDate(Long.toString(value.date)).build();

            context.write(new AvroKey<Sale>(sale), NullWritable.get());
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
