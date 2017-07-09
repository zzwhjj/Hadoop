package cn.zzw.hadoop.hdfs;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

public class HDFSDemo {
	FileSystem fs = null;

	@Before
	public void ini() throws IOException, URISyntaxException, InterruptedException {
		fs = FileSystem.get(new URI("hdfs://192.168.31.130:9000"), new Configuration(), "root");
	}

	@Test
	public void testUpload() throws Exception {
		InputStream in = new FileInputStream("C:/test.txt");
		OutputStream out = fs.create(new Path("/data/input/kpi-test.log"));
		IOUtils.copyBytes(in, out, 4096, true);
	}

	@Test
	public void testDownload() throws IllegalArgumentException, IOException {
		fs.copyToLocalFile(new Path("/data/input/kpi-test.log"), new Path("C:/zzw.log"));
	}

	@Test
	public void testDel() throws Exception, IOException {
		boolean flag = fs.delete(new Path("/data/input/1.log"), false);
		System.out.println(flag);
	}

	@Test
	public void testMkdir() throws IllegalArgumentException, IOException {
		boolean mkdirs = fs.mkdirs(new Path("/test"));
		System.out.println(mkdirs);
	}

	public static void main(String[] args) throws Exception {
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.31.130:9000"), new Configuration());
		InputStream in = fs.open(new Path("/data/input/test.log"));
		OutputStream out = new FileOutputStream("C:/test.txt");
		IOUtils.copyBytes(in, out, 4096, true);
	}
}
