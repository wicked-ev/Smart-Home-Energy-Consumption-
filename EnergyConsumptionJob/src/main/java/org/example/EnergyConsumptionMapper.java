import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class EnergyConsumptionMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Split the input line by commas
        String line = value.toString();
        String[] columns = line.split(",");

        // Skip the header row
        if (columns[0].equals("UnixTimestamp")) {
            return;
        }

        try {
            // Parse energy consumption (column index 10)
            double energyConsumption = Double.parseDouble(columns[10]);

            // Emit key-value pairs for each appliance and its corresponding energy consumption
            if (columns[2].equals("1")) context.write(new Text("Television"), new DoubleWritable(energyConsumption));
            if (columns[3].equals("1")) context.write(new Text("Dryer"), new DoubleWritable(energyConsumption));
            if (columns[4].equals("1")) context.write(new Text("Oven"), new DoubleWritable(energyConsumption));
            if (columns[5].equals("1")) context.write(new Text("Refrigerator"), new DoubleWritable(energyConsumption));
            if (columns[6].equals("1")) context.write(new Text("Microwave"), new DoubleWritable(energyConsumption));
        } catch (NumberFormatException e) {
            System.err.println("Error parsing line: " + line);
        }
    }
}
