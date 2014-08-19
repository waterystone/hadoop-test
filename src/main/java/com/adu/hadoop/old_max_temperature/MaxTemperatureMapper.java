package com.adu.hadoop.old_max_temperature;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MaxTemperatureMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		//key为该行在输入文件的偏移(字节),vaule为该行的内容。
		String line = value.toString();
		String[] pair = line.split(",");
		String year = pair[0];
		int temperature = Integer.parseInt(pair[1]);
		//输出year<->temprature.
		output.collect(new Text(year), new IntWritable(temperature));

	}

}
