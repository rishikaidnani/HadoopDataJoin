/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Business;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author rishikaidnani
 */
public class MapReduceOne extends Configured implements Tool {

    public static class Mapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Text wordText = new Text();
        private static IntWritable uno = new IntWritable(1);

        public void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
            String[] words = line.toString().split("\\s");
            for (String word : words) {
                wordText.set(word);
                context.write(wordText, uno);
            }
        }
    }

    public static class Reducer1 extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text word, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int total = 0;
            int count = 2;
            for (IntWritable uno : values) {
                total += uno.get();
            }
            result.set(total);
            context.write(word, result);
        }
    }

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = getConf();
        Job job = new Job(conf, "FirstJob");
        job.setJarByClass(MapReduceOne.class);

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

        job.setMapperClass(Mapper1.class);
        job.setCombinerClass(Reducer1.class);
        job.setReducerClass(Reducer1.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new MapReduceOne(), args);
        System.exit(res);
    }

}
