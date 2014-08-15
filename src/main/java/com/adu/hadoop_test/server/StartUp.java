package com.adu.hadoop_test.server;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class StartUp {
	private static Log logger = LogFactory.getLog(StartUp.class);

	public static void main(String[] args) throws Exception {
		String filename = "/user/adrd-dev/yunjiedu/README.txt";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		Path path = new Path(filename);
		if (fs.exists(path)) {
			FSDataInputStream is = fs.open(path);
			FileStatus stat = fs.getFileStatus(path);

			byte[] buffer = new byte[Integer.parseInt(String.valueOf(stat
					.getLen()))];
			is.readFully(0, buffer);

			is.close();
			fs.close();
			logger.debug(new String(buffer));

		} else {
			throw new Exception("the file is not found .");
		}
	}
}
