package com.adu.hadoop_test.util;

import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HadoopUtil {

	private static Log logger = LogFactory.getLog(HadoopUtil.class);

	public static byte[] read(String filePath) {
		Configuration conf = new Configuration();
		String prefix = "hdfs://10.10.70.155:9000";
		String uri = prefix + filePath;

		byte[] buffer = null;
		try {
			FileSystem fileSystem = FileSystem.get(URI.create(uri), conf);

			Path path = new Path(uri);
			if (fileSystem.exists(path)) {
				FSDataInputStream in = fileSystem.open(path);
				FileStatus stat = fileSystem.getFileStatus(path);

				buffer = new byte[Integer
						.parseInt(String.valueOf(stat.getLen()))];
				in.readFully(0, buffer);

				in.close();
				fileSystem.close();
			} else {
				throw new Exception("the file is not found .");
			}
		} catch (Exception e) {
			logger.error("[ERROR-read]filename=" + filePath, e);
		}
		return buffer;
	}
}
