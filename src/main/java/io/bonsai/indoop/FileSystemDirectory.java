package io.bonsai.indoop;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.NoLockFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

/**
 * A directory on a Hadoop FileSystem
 * 
 * @author kyle
 * 
 */
public class FileSystemDirectory extends Directory {

	static class BlockKey {

		private final String filename;
		private final long pos;

		public BlockKey(final String filename, final long pos) {
			this.filename = filename;
			this.pos = pos;
		}

		@Override
		public String toString() {
			return "BlockKey [filename=" + filename + ", pos=" + pos + "]";
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result
					+ ((filename == null) ? 0 : filename.hashCode());
			result = prime * result + (int) (pos ^ (pos >>> 32));
			return result;
		}

		@Override
		public boolean equals(final Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			final BlockKey other = (BlockKey) obj;
			if (filename == null) {
				if (other.filename != null)
					return false;
			} else if (!filename.equals(other.filename))
				return false;
			if (pos != other.pos)
				return false;
			return true;
		}
	}

	static class BlockValue {
		private final MappedByteBuffer bb;
		private RandomAccessFile raf;
		private final File file;

		public BlockValue(final File file, final RandomAccessFile raf,
				final MappedByteBuffer bb) throws IOException {
			this.bb = bb;
			this.file = file;
		}

		public void destroy() throws IOException {
			raf.close();
			file.delete();
		}

		public ByteBuffer getByteBuffer() {
			return bb;
		}
	}

	protected Path root;
	private final FileSystem fs;
	private final Set<String> writes;
	private final ConcurrentHashMap<String, Long> lengthCache;
	private boolean closed;
	private final int bufferSize;
	private final Cache<BlockKey, BlockValue> blockCache;
	private final File tmp;
	private boolean listAllCalled;
	private static final Logger log = LoggerFactory
			.getLogger(FileSystemDirectory.class);
	private static final int DEFAULT_BUFFER_SIZE = 1 << 20;
	private static final RemovalListener<BlockKey, BlockValue> BLOCK_CACHE_REMOVAL = new RemovalListener<BlockKey, BlockValue>() {
		@Override
		public void onRemoval(
				final RemovalNotification<BlockKey, BlockValue> notification) {
			try {
				notification.getValue().destroy();
			} catch (final IOException e) {
				throw new RuntimeException(e);
			}
		}
	};

	class FileSystemIndexOutput extends IndexOutput {

		private final String filename;
		private final File tempfile;
		private final RandomAccessFile scratch;

		public FileSystemIndexOutput(final String filename,
				final IOContext context) throws IOException {
			this.filename = filename;
			this.tempfile = File.createTempFile("temp", ".fsd");
			this.tempfile.deleteOnExit();
			this.scratch = new RandomAccessFile(tempfile, "rw");
		}

		@Override
		public void close() throws IOException {
			flush();
			log.debug("closing " + filename + " (" + scratch.length()
					+ " bytes)");
			try {
				fs.copyFromLocalFile(new Path(tempfile.getAbsolutePath()),
						new Path(root, filename));
			} finally {
				writes.remove(filename);
				synchronized (writes) {
					writes.notify();
				}
			}
			scratch.close();
		}

		@Override
		public void flush() throws IOException {
			try {
				scratch.getFD().sync();
			} catch (final IOException e) {
				System.out.println("WUT");
			}
		}

		@Override
		public long getFilePointer() {
			try {
				return scratch.getFilePointer();
			} catch (final IOException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public long length() throws IOException {
			return scratch.length();
		}

		@Override
		public void seek(final long pos) throws IOException {
			scratch.seek(pos);
		}

		@Override
		public void writeByte(final byte arg0) throws IOException {
			scratch.writeByte(arg0);
		}

		@Override
		public void writeBytes(final byte[] arg0, final int arg1, final int arg2)
				throws IOException {
			scratch.write(arg0, arg1, arg2);
		}
	}

	class FileSystemIndexInput extends IndexInput {

		private final String filename;
		private final List<FileSystemIndexInput> children = Collections
				.synchronizedList(new ArrayList<FileSystemIndexInput>());;
		private final long len;
		private ByteBuffer buffer;
		private long bufferOffset;
		private long pos;
		private final int bufferSize;

		public FileSystemIndexInput(final String filename,
				final IOContext context, final long len, final int bufferSize)
				throws IOException {
			super(filename);
			this.len = len;
			this.filename = filename;
			this.bufferSize = bufferSize;
			seek(0);
		}

		public FileSystemIndexInput(final String filename,
				final long filePointer, final ByteBuffer buffer,
				final long bufferOffset, final long len, final int bufferSize)
				throws IOException {
			super(filename);
			this.buffer = buffer;
			this.bufferOffset = bufferOffset;
			this.len = len;
			this.filename = filename;
			this.bufferSize = bufferSize;
			seek(filePointer);
		}

		@Override
		public void close() throws IOException {
		}

		@Override
		public long getFilePointer() {
			return pos;
		}

		@Override
		public long length() {
			return len;
		}

		@Override
		public void seek(final long to) throws IOException {
			pos = to;
		}

		@Override
		public void readBytes(final byte[] bytes, int offset, int len)
				throws IOException {
			while (len > 0) {
				if (buffer == null || pos < bufferOffset
						|| pos >= bufferOffset + bufferSize) {
					try {
						bufferOffset = (pos / bufferSize) * bufferSize;
						final BlockKey key = new BlockKey(filename,
								bufferOffset);
						buffer = blockCache.get(key, blockCacheLoader(key))
								.getByteBuffer();
					} catch (final ExecutionException e) {
						throw new RuntimeException(e);
					}
				}
				final ByteBuffer buf = buffer.duplicate();
				buf.position((int) (pos - bufferOffset));
				final int remaining = buf.remaining();
				if (remaining == 0) {
					if (buf.position() == 0) {
						log.warn("reading zero-length bytebuffer, returning");
					}
					seek(pos);
				} else {
					final int count = len > remaining ? remaining : len;
					buf.get(bytes, offset, count);
					offset += count;
					pos += count;
					len -= count;
				}
			}
		}

		private Callable<BlockValue> blockCacheLoader(final BlockKey key) {
			return new Callable<BlockValue>() {
				@Override
				public BlockValue call() throws Exception {
					log.debug("cache loader for " + key);
					final long remaining = length() - getFilePointer();
					final int fillable = (int) (remaining > bufferSize ? bufferSize
							: remaining);
					final File file = new File(tmp, filename + "." + pos);
					final long fileLength = file.length();
					final RandomAccessFile raf = new RandomAccessFile(file,
							"rw");
					final MappedByteBuffer bb = raf.getChannel().map(
							FileChannel.MapMode.READ_WRITE, 0, fillable);
					if (fileLength != fillable || filename.endsWith(".gen")) {
						log.debug("reloading " + key);
						final FSDataInputStream input = fs.open(new Path(root,
								filename));
						input.seek(pos);
						read(input, bb, fillable);
						input.close();
					} else {
						log.debug("found locally: " + key);
					}
					return new BlockValue(file, raf, bb);
				}
			};
		}

		private void read(final FSDataInputStream input, final byte[] bytes,
				int len) throws IOException {
			int off = 0;
			while (len > 0) {
				final int count = input.read(bytes, off, len);
				off += count;
				len -= count;
			}
		}

		private void read(final FSDataInputStream input, final ByteBuffer bb,
				final int len) throws IOException {
			final byte[] bytes = new byte[len];
			read(input, bytes, len);
			bb.put(bytes, 0, len);
			bb.flip();
		}

		@Override
		public byte readByte() throws IOException {
			final byte[] b = new byte[1];
			readBytes(b, 0, 1);
			return b[0];
		}

		@Override
		public IndexInput clone() {
			try {
				final FileSystemIndexInput child = new FileSystemIndexInput(
						filename, getFilePointer(), buffer, bufferOffset, len,
						bufferSize);
				children.add(child);
				return child;
			} catch (final IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	public FileSystemDirectory(final Path root, Configuration conf, File tmpdir)
			throws IOException {
		this(root, DEFAULT_BUFFER_SIZE, conf, tmpdir);
	}

	public FileSystemDirectory(final Path root, final int bufferSize,
			Configuration conf, File tmpdir) throws IOException {
		this.bufferSize = bufferSize;
		this.fs = root.getFileSystem(conf);
		this.root = root;
		log.debug("initializing fsdir at " + root);
		fs.mkdirs(root);
		this.writes = Collections.synchronizedSet(new TreeSet<String>());
		this.setLockFactory(NoLockFactory.getNoLockFactory());
		this.blockCache = CacheBuilder.newBuilder().maximumSize(1024)
				.removalListener(BLOCK_CACHE_REMOVAL)
				.expireAfterAccess(1, TimeUnit.HOURS).build();
		this.lengthCache = new ConcurrentHashMap<String, Long>();
		File cacheRoot = new File(tmpdir, "leaf-cache");
		tmp = new File(cacheRoot, this.getLockID());
		tmp.mkdirs();
		log.debug("initialized fsdir at " + root);
	}

	@Override
	public String getLockID() {
		return root.getName();
	}

	@Override
	public String[] listAll() throws IOException {
		final FileStatus[] statuses = fs.listStatus(root, new PathFilter() {
			@Override
			public boolean accept(final Path name) {
				return !root.equals(name)
						&& !name.getName().equals("segments.gen");
			}
		});
		final ArrayList<String> names = new ArrayList<String>();
		for (int i = 0; i < statuses.length; i++) {
			final long len = statuses[i].getLen();
			if (len > 0) {
				final String name = statuses[i].getPath().getName();
				names.add(name);
				lengthCache.put(name, len);
			}
		}
		listAllCalled = true;
		return names.toArray(new String[0]);
	}

	@Override
	public boolean fileExists(final String name) throws IOException {
		if (!listAllCalled) {
			listAll();
		}
		return lengthCache.contains(name);
	}

	@Override
	public void deleteFile(final String name) throws IOException {
		lengthCache.remove(name);
		fs.delete(new Path(root, name), false);
	}

	@Override
	public long fileLength(final String name) throws IOException {
		Long len = lengthCache.get(name);
		if (len == null) {
			FileStatus status = fs.getFileStatus(new Path(root, name));
			if (status == null) {
				throw new FileNotFoundException(name);
			}
			len = status.getLen();
			lengthCache.put(name, len);
		}
		return len;
	}

	@Override
	public IndexOutput createOutput(final String name, final IOContext context)
			throws IOException {
		writes.add(name);
		return new FileSystemIndexOutput(name, context);
	}

	@Override
	public void sync(final Collection<String> files) throws IOException {
		while (containsAny(files, writes)) {
			try {
				writes.wait();
			} catch (final InterruptedException e) {
				throw new RuntimeException(e);
			}
		}
	}

	private boolean containsAny(final Collection<String> haystack,
			final Set<String> needles) {
		for (final String needle : needles) {
			if (haystack.contains(needle)) {
				return true;
			}
		}
		return false;
	}

	@Override
	public IndexInput openInput(final String filename, final IOContext context)
			throws IOException {
		return new FileSystemIndexInput(filename, context,
				fileLength(filename), bufferSize);
	}

	@Override
	public synchronized void close() throws IOException {
		if (closed) {
			return;
		}
		while (writes.size() > 0) {
			try {
				synchronized (writes) {
					log.debug("waiting on " + writes);
					writes.wait();
					log.debug("released");
				}
			} catch (final InterruptedException e) {
				throw new RuntimeException(e);
			}
		}
		fs.close();
		closed = true;
	}
}
