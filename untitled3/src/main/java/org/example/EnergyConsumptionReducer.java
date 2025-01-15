import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class EnergyConsumptionReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double totalConsumption = 0;

        // Sum up all the energy consumption values for the given appliance
        for (DoubleWritable value : values) {
            totalConsumption += value.get();
        }

        // Emit the appliance name and its total energy consumption
        context.write(key, new DoubleWritable(totalConsumption));
    }
}

