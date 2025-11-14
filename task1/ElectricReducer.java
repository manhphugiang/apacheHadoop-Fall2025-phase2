package projPhase1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class ElectricReducer extends Reducer<Text, Text, Text, Text> {
    private MultipleOutputs<Text, Text> dateOutput;

    @Override
    protected void setup(Context context) {
        dateOutput = new MultipleOutputs<>(context);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> records, Context context) throws IOException, InterruptedException {
        String keyStr = key.toString();  // house and date - 1/2012-06-01
        String[] parts = keyStr.split("/");
        if (parts.length < 2) return;

        String houseId = parts[0];       // "1"
        String date = parts[1];          // "2012-06-01"
        String datePath = date.replace("-", "/");  // "2012/06/01" - change the date format
        String fileName = "datafile_" + date.replace("-", "_");

        // Write all complete records to date-organized structure
        for (Text record : records) {
            dateOutput.write(key, record, datePath + "/" + fileName);
        }
        // time series directory structure ///Dataset/YYYY/MM/DD/datafile_YYYY_MM_DD.txt
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        dateOutput.close();
    }
}
