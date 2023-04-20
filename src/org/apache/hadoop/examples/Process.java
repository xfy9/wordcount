package org.apache.hadoop.examples;






 

import java.io.IOException;  
import java.util.StringTokenizer;   
import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.IntWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.Mapper;  
import org.apache.hadoop.mapreduce.Reducer;  
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  
import org.apache.hadoop.util.GenericOptionsParser;    
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
 




public class Process {
	
    public Process() {
    }

    
    
    public static void main(String[] args) throws Exception{  
        // TODO Auto-generated method stub  
        Configuration conf = new Configuration();  
        String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();  
        if(otherArgs.length != 2){  
            System.err.println("Usage WordCount <int> <out>");  
            System.exit(2);  
        }  
        Job job = Job.getInstance(conf, "Data count");
        job.setJarByClass(Process.class);
        job.setMapperClass(DedupMapper.class);  
        job.setCombinerClass(DedupReducer.class);  
        job.setReducerClass(DedupReducer.class);  
        job.setOutputKeyClass(Text.class);  
        job.setOutputValueClass(Text.class);  
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));  
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));  
        System.exit(job.waitForCompletion(true) ? 0 : 1);  
    }    
 







   
}
