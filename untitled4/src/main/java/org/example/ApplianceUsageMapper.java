

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ApplianceUsageMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text appliance = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Split the input line into fields (assumes CSV format)
        String[] fields = value.toString().split(",");

        // Appliances columns: Television, Dryer, Oven, Refrigerator, Microwave
        String[] applianceNames = {"Television", "Dryer", "Oven", "Refrigerator", "Microwave"};

        for (int i = 2; i <= 6; i++) { // Indices of appliance usage columns
            if (fields.length > i && fields[i].trim().equals("1")) {
                appliance.set(applianceNames[i - 2]); // Map column index to appliance name
                context.write(appliance, one);
            }
        }
    }
}
