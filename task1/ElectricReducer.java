package projPhase1;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class ElectricReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    private MultipleOutputs<Text, FloatWritable> dateOutput;

    @Override
    protected void setup(Context context) {
        dateOutput = new MultipleOutputs<>(context);
    }

    @Override
    protected void reduce(Text key, Iterable<FloatWritable> readings, Context context) throws IOException, InterruptedException {
        String keyStr = key.toString();  // house and date - 1/2012-06-01
        String[] parts = keyStr.split("/");
        if (parts.length < 2) return;

        String houseId = parts[0];       // "1"
        String date = parts[1];          // "2012-06-01"
        String datePath = date.replace("-", "/");  // "2012/06/01" - change the date format
        String fileName = "datafile_" + date.replace("-", "_");

        // Just write all readings to date-organized structure, no calculation
        for (FloatWritable reading : readings) {
            dateOutput.write(key, reading, datePath + "/" + fileName);
        }
        // time series directory structure ///Dataset/YYYY/MM/DD/datafile_YYYY_MM_DD.txt
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        dateOutput.close();
    }
}
