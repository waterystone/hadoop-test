package com.adu.hadoop.fs;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

public class FileSystemTest {

	private Log logger = LogFactory.getLog(this.getClass());

	@Test
	public void open() throws IOException {
		String uri = "hdfs://10.16.10.66:9001/user/root/input/README.txt";
		logger.debug("uri=" + uri);
		URI.create(uri);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		InputStream in = null;
		logger.debug("ok");
		try {
			in = fs.open(new Path(uri));
			IOUtils.copyBytes(in, System.out, 4096, false);
		} finally {
			IOUtils.closeStream(in);
		}
	}

}
