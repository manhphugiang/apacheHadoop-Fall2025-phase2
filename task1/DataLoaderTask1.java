import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

// Mapper Class
class RawDataMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Skip header line
        if (key.get() == 0 && value.toString().contains("LOG_ID")) {
            return;
        }
        
        String[] values = value.toString().split("\\t");
        if (values.length >= 3) {
            String houseId_condate = values[1] + "/" + values[2]; // house_id/date
            context.write(new Text(houseId_condate), value);
        }
    }
}

// Reducer Class  
class RawDataReducer extends Reducer<Text, Text, Text, Text> {
    private MultipleOutputs<Text, Text> dateOutput;
    
    @Override
    protected void setup(Context context) {
        dateOutput = new MultipleOutputs<>(context);
    }
    
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            String keyStr = key.toString(); // house_id/date - 1/2012-06-01
            String[] parts = keyStr.split("/");
            if (parts.length < 2) return;
            
            String date = parts[1]; // 2012-06-01
            String datePath = date.replace("-", "/"); // 2012/06/01
            String fileName = "datafile_" + date.replace("-", "_");
            
            dateOutput.write(key, value, datePath + "/" + fileName);
        }
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        dateOutput.close();
    }
}

// Main Job Class
public class DataLoaderTask1 {
    public static void main(String[] args) throws Exception {
        new DataLoaderTask1().run(args);
    }
    
    public void run(String[] args) throws Exception {
        Path inPath = new Path(args[0]);
        Path outPath = new Path(args[1]);
        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        
        job.setJarByClass(DataLoaderTask1.class);
        job.setJobName("Task 1: Consumption Data Loader to Phase 1 Structure");
        job.setMapperClass(RawDataMapper.class);
        job.setReducerClass(RawDataReducer.class);
        job.setNumReduceTasks(4);
        
        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
