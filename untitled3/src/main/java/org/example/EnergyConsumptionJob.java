import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class EnergyConsumptionJob {
    public static void main(String[] args) throws Exception {
        // Check input arguments
        if (args.length != 2) {
            System.err.println("Usage: EnergyConsumptionJob <input path> <output path>");
            System.exit(-1);
        }

        // Set up the Hadoop job configuration
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Energy Consumption Calculation");

        // Specify the Jar file containing the driver, mapper, and reducer
        job.setJarByClass(EnergyConsumptionJob.class);

        // Set the mapper and reducer classes
        job.setMapperClass(EnergyConsumptionMapper.class);
        job.setReducerClass(EnergyConsumptionReducer.class);

        // Specify the output key and value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // Specify the input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Exit upon completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
