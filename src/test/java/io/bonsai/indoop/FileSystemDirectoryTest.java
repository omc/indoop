package io.bonsai.indoop;

import static org.junit.Assert.assertEquals;
import io.bonsai.indoop.FileSystemDirectory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.junit.Test;

public class FileSystemDirectoryTest {

	@Test
	public void test() throws IOException {
		final File tmp = new File("tmp");
		tmp.mkdirs();
		FileUtil.fullyDeleteContents(tmp);

		final FileWriter writer = new FileWriter(new File(tmp, "text"));
		for (int i = 0; i < 1000; i++) {
			writer.append("0123456");
		}
		final char base = '0';
		writer.close();
		final FileSystemDirectory dir = new FileSystemDirectory(
				new Path("tmp"), 500, new Configuration(), new File(
						System.getProperty("java.io.tmpdir")));
		final IndexInput input = dir.openInput("text", IOContext.DEFAULT);
		final byte[] buffer = new byte[7000];
		input.readBytes(buffer, 0, 7000);
		for (int i = 0; i < 7000; i++) {
			assertEquals("mismatch at " + i + " was " + buffer[i]
					+ " should have been " + i % 7, i % 7, buffer[i] - base);
		}
	}
}
