package assignment2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class IndiaMedalsPerYear {
    
    //mapper class
    public static class OlympicMedalCountMapper extends
Mapper<LongWritable, Text, Text, IntWritable> {
        
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            
            String line = value.toString();
            
            if (line.length() > 0) {
                
                int countryIndex = line.indexOf("India");
                
                if (countryIndex > 0) {
                    String[] fields = line.split("\t");
                    
                    String year = fields[3];
                    int totalMedals = Integer.parseInt(fields[9]);
                    
                    context.write(new Text(year), new IntWritable(totalMedals));
                }                
            }
        }
    }
    
    //reducer class
    public static class OlympicMedalCountReducer extends Reducer<Text,
IntWritable, Text, IntWritable> {
        
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            
            int totalMedals = 0;
            
            for (IntWritable value : values) {
                totalMedals = totalMedals + value.get();
            }
            
            context.write(key, new IntWritable(totalMedals));
        }
        
    }
    
    //driver class
    public static void main(String[] args) throws IOException,
ClassNotFoundException, InterruptedException {
        
        Configuration conf = new Configuration();
        Job job = new Job();
        job.setJarByClass(IndiaMedalsPerYear.class);
        job.setJobName("Olympic Medal Count");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(OlympicMedalCountMapper.class);
        job.setReducerClass(OlympicMedalCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        Path out = new Path(args[1]);
        out.getFileSystem(conf).delete(out);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        
    }

}