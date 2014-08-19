package com.adu.hadoop.max_temperature;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTemperatureMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// key为该行在输入文件的偏移(字节),vaule为该行的内容。
		String line = value.toString();
		String[] pair = line.split(",");
		String year = pair[0];
		int temperature = Integer.parseInt(pair[1]);
		// 输出year<->temprature.
		context.write(new Text(year), new IntWritable(temperature));
	}
}
