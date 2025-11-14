package projPhase1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ElectricMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        if (key.get() == 0 && value.toString().contains("LOG_ID")) {
            return;
        }
        String[] values = value.toString().split("\\t");
        String houseId_condate;
        try {
            houseId_condate = values[1] + "/" + values[2]; // output as houseId/condate
        }
        catch (Exception e) {
            houseId_condate = "9999/9999";
        }
        // Pass the complete record, not just energy reading
        context.write(new Text(houseId_condate), value);
    }
}
