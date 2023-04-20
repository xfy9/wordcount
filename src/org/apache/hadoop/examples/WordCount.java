package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
 
public class WordCount {
    public WordCount() {
    }
 
//    public static void main(String[] args) throws Exception {
//        Configuration conf = new Configuration();
//        String[] otherArgs = (new GenericOptionsParser(conf, args)).getRemainingArgs();
//        if(otherArgs.length < 2) {
//            System.err.println("Usage: wordcount <in> [<in>...] <out>");
//            System.exit(2);
//        }
// 
//        Job job = Job.getInstance(conf, "word count");
//        job.setJarByClass(WordCount.class);
//        job.setMapperClass(WordCount.TokenizerMapper.class);
//        job.setCombinerClass(WordCount.IntSumReducer.class);
//        job.setReducerClass(WordCount.IntSumReducer.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(IntWritable.class);
// 
//        for(int i = 0; i < otherArgs.length - 1; ++i) {
//            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
//        }
// 
//        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
//        System.exit(job.waitForCompletion(true)?0:1);
//    }
// 
//    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
//        private IntWritable result = new IntWritable();
// 
//        public IntSumReducer() {
//        }
// 
//        public void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
//            int sum = 0;
// 
//            IntWritable val;
//            for(Iterator i$ = values.iterator(); i$.hasNext(); sum += val.get()) {
//                val = (IntWritable)i$.next();
//            }
// 
//            this.result.set(sum);
//            context.write(key, this.result);
//        }
//    }
// 
//    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
//        private static final IntWritable one = new IntWritable(1);
//        private Text word = new Text();
// 
//        public TokenizerMapper() {
//        }
// 
//        public void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
//            StringTokenizer itr = new StringTokenizer(value.toString());
// 
//            while(itr.hasMoreTokens()) {
//                this.word.set(itr.nextToken());
//                context.write(this.word, one);
//            }
// 
//        }
//    }

    
    
    public static void main(String[] args) throws Exception{  
        // TODO Auto-generated method stub  
        Configuration conf = new Configuration();  
        String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();  
        if(otherArgs.length != 2){  
            System.err.println("Usage WordCount <int> <out>");  
            System.exit(2);  
        }  
        Job job = Job.getInstance(conf, "Data count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(WordCount.DupMap.class);  
        job.setCombinerClass(WordCount.DupReduce.class);  
        job.setReducerClass(WordCount.DupReduce.class);  
        job.setOutputKeyClass(Text.class);  
        job.setOutputValueClass(Text.class);  
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));  
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));  
        System.exit(job.waitForCompletion(true) ? 0 : 1);  
    }    
 
	 //map将输入中的value复制到输出数据的key上，并直接输出  
    public static class DupMap extends Mapper<Object,Text,Text,Text>{
    	public DupMap() {
			// TODO Auto-generated constructor stub
		}
    	
        private static Text line = new Text();  
        public void map(Object key,Text value,Context context) throws IOException,InterruptedException{  
            line = value;  
            context.write(line, new Text(""));  
        }  
    }  
    //reduce将输入中的key复制到输出数据的key上，并直接输出  
    public static class DupReduce extends Reducer<Text,Text,Text,Text>{
    	
    	public DupReduce() {
    		
    	}
    	
        public void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException{  
            context.write(key, new Text(""));  	              
        }  
    }  
}
