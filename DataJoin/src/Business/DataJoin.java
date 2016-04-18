/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Business;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.contrib.utils.join.DataJoinMapperBase;
import org.apache.hadoop.contrib.utils.join.DataJoinReducerBase;
import org.apache.hadoop.contrib.utils.join.TaggedMapOutput;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author rishikaidnani
 */
public class DataJoin extends Configured implements Tool {

    public static class TaggedWritable extends TaggedMapOutput {

        private Writable data;

        public TaggedWritable() {
            this(new Text(""));
        }

        public TaggedWritable(Writable data) {
            this.tag = new Text("");
            this.data = data;
        }

        @Override
        public Writable getData() {
            return data;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            this.tag.write(out);
            this.data.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.tag.readFields(in);
            this.data.readFields(in);
        }

    }

    public static class MapClass extends DataJoinMapperBase {

        //For example, customers and orders that are tables are tags
        @Override
        protected Text generateInputTag(String inputFile) {
            String dataTable = inputFile.split("-")[0];
            return new Text(dataTable);
        }

        @Override
        protected TaggedMapOutput generateTaggedMapOutput(Object value) {
            TaggedWritable retv = new TaggedWritable((Text) value);
            retv.setTag(this.inputTag);
            return retv;
        }

        //Generate primary key for join
        @Override
        protected Text generateGroupKey(TaggedMapOutput aRecord) {
            String line = ((Text) aRecord.getData()).toString();
            String primaryKey = line.split(",")[0];
            return new Text(primaryKey);
        }

    }
//Group key = 3
//tags = {"Customers", "Orders"};
//values = {"3,Jose Madriz,281-330-8004", "A,12.95,02-Jun-2008"};

    public static class ReduceClass extends DataJoinReducerBase {

        @Override
        protected TaggedMapOutput combine(Object[] tags, Object[] values) {
            if (tags.length < 2) {
                //only 1 table present
                return null;
            }
            String joinedString = "";
            for (int i = 0; i < values.length; i++) {
                if (i > 0) {
                    joinedString += ",";
                }
                TaggedWritable tw = (TaggedWritable) values[i];
                String line = ((Text) tw.getData()).toString();
                String[] keyAndNonKeyPart = line.split(",", 2);
                joinedString += keyAndNonKeyPart[1];
            }
            TaggedWritable retv = new TaggedWritable(new Text(joinedString));
            retv.setTag((Text) tags[0]);
            return retv;
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        JobConf job = new JobConf(conf, DataJoin.class);

        final File f = new File(MapReduceOne.class.getProtectionDomain().getCodeSource().getLocation().getPath());
        String inFiles = f.getAbsolutePath().replace("/build/classes", "") + "/src/inFiles/";
        String outFiles = f.getAbsolutePath().replace("/build/classes", "") + "/src/outFiles/OutputOne";
        //use the arguments instead if provided.
        if (args.length > 1) {
            inFiles = args[1];
            outFiles = args[2];
        }
        Path in = new Path(inFiles);
        Path out = new Path(outFiles);
        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);

        job.setJobName("Data Join");
        job.setMapperClass(MapClass.class);
        job.setReducerClass(ReduceClass.class);

        job.setInputFormat(TextInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TaggedWritable.class);
        job.set("mapred.textoutputformat.separator", ",");

        JobClient.runJob(job);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new DataJoin(), args);
        System.exit(res);
    }

}
