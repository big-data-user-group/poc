package org.bd.poc;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapred.lib.db.DBInputFormat;
import org.apache.hadoop.mapred.lib.db.DBWritable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class Importer extends Configured implements Tool {

    private static final Log LOG = LogFactory.getLog(Importer.class);

    public int run(String[] args) throws Exception {
        JobConf conf = new JobConf(getConf(), Importer.class);
        conf.setJobName("Importer");

        conf.setOutputKeyClass(NullWritable.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(Map.class);

        Configuration jdbcConfig = new Configuration();
        jdbcConfig.addResource("jdbc-default.xml");
        jdbcConfig.addResource("jdbc.xml");

        LOG.info(String.format("jdbc.properties driver: %s, url: %s, user: %s, password: %s",
                jdbcConfig.get("jdbc.driverClassName"),
                jdbcConfig.get("jdbc.url"),
                jdbcConfig.get("jdbc.username"),
                jdbcConfig.get("jdbc.password")
        ));

        conf.setInputFormat(DBInputFormat.class);
        DBConfiguration.configureDB(conf, jdbcConfig.get("jdbc.driverClassName"), jdbcConfig.get("jdbc.url"),
                jdbcConfig.get("jdbc.username"), jdbcConfig.get("jdbc.password"));

        DBInputFormat.setInput(conf, DBRecord.class,
                "SELECT product_id, office_location_id, sold_price, date FROM sale", "SELECT COUNT(*) FROM sale");

        conf.setOutputFormat(TextOutputFormat.class);

        FileOutputFormat.setOutputPath(conf, new Path(args[0]));

        JobClient.runJob(conf);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Importer(), args);
        System.exit(res);
    }

    public static class Map extends MapReduceBase implements Mapper<LongWritable, DBRecord, NullWritable, Text> {

        public void map(LongWritable key, DBRecord value, OutputCollector<NullWritable, Text> output, Reporter reporter) throws IOException {
            List<String> fields = Lists.newArrayList(Long.toString(value.date), Long.toString(value.productID), Long.toString(value.locationID), value.totalPrice);
            String outLine = Joiner.on(",").join(fields);
            output.collect(NullWritable.get(), new Text(outLine));
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
