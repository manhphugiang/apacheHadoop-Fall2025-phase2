package projPhase1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ReduceJob {
    public static void main(String[] args) throws Exception {
        new ReduceJob().run(args);
    }

    public void run(String[] args) throws Exception {
        Path inPath = new Path(args[0]);
        Path outPath = new Path(args[1]);
        System.out.println(inPath.toString());
        System.out.println(outPath.toString());
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(ReduceJob.class);
        job.setJobName("Map and Reduce Job and Save Data To HDFS time series file structure");

        job.setMapperClass(ElectricMapper.class);
        job.setReducerClass(ElectricReducer.class);

        job.setNumReduceTasks(4);

        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
