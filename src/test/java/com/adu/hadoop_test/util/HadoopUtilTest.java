package com.adu.hadoop_test.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

public class HadoopUtilTest {
	private Log logger = LogFactory.getLog(this.getClass());

	@Test
	public void read() {
		String filePath = "/user/adrd-dev/yunjiedu/README.txt";
		byte[] res = HadoopUtil.read(filePath);
		logger.debug("res=" + new String(res));
	}
}
