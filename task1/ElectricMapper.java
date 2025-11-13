package projPhase1;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ElectricMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        if (key.get() == 0 && value.toString().contains("LOG_ID")) {
            return;
        }
        String[] values = value.toString().split("\\t");
        String houseId_condate;
        float energy_reading;
        try {
            houseId_condate = values[1] + "/" + values[2]; // output as houseId/condate
            energy_reading = Float.parseFloat(values[4]);
        }
        catch (Exception e) {
            houseId_condate = "9999/9999";
            energy_reading = 0;
        }
        context.write(new Text(houseId_condate), new FloatWritable(energy_reading));
    }
}
