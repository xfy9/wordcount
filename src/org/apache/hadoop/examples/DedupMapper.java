package org.apache.hadoop.examples;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DedupMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	private  Text line = new Text();//每行数据
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)throws IOException, InterruptedException {
		line = value;
		context.write(line,NullWritable.get());	
	}


}
