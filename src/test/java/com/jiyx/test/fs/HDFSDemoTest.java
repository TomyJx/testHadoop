package com.jiyx.test.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

/**
 * 只测试了部分方法，其他的请自行测试
 *
 * @author jiyx
 * @create 2018-09-27-18:50
 */
@Ignore
public class HDFSDemoTest {

	private FileSystem fs = null;
	private static final long M_100 = 1024 * 1024 * 100;

	/**
	 * 初始化
	 * @throws Exception
	 */
	@Before
	public void init() throws Exception {
		// 最后一个参数其实就是伪装成root用户创建了这个链接
		// 那么接下来的所有动作都是root用户在干了，不会出现pemission denied错误了
		// 正常生产中需要使用kerberos做权限框架的，不然那任何一个人都可以伪装成root用户操作
		fs = FileSystem.get(new URI("hdfs://hadoop01:9000"), new Configuration(), "root");
//		fs = FileSystem.get(new URI("hdfs://hadoop01:9000"), new Configuration(), "hadoop01");
//		fs = FileSystem.get(new URI("hdfs://hadoop01:9000"), new Configuration());
	}

	@After
	public void destory() throws IOException {
		fs.close();
	}

	/**
	 * 创建目录
	 * @throws Exception
	 */
	@Test
	public void testMkdir() throws Exception {
		// mkdirs 创建文件夹，文件夹默认权限drwxr-xr-x
		// 如果目录存在，这里也会返回true，但是不会创建一个新的目录去覆盖原目录
		// 所以如果存在/aa/bb，重新创建/aa的时候不会把bb给覆盖没了
		boolean mkdirs = fs.mkdirs(new Path("/mk1/"));
		System.out.println(mkdirs);
		// mkdirs 可以创建多级目录
		boolean mkdirs1 = fs.mkdirs(new Path("/mk1/1/2/3"));
		System.out.println(mkdirs + " " + mkdirs1);
	}

	/**
	 * 创建文件
	 * @throws Exception
	 */
	@Test
	public void testCreate() throws Exception {
		FSDataOutputStream fsOut = null;
		// 判断文件、目录是否存在
		boolean exists = fs.exists(new Path("/cc/a.txt"));
		System.out.println("existsFile = " + exists);
		exists = fs.exists(new Path("/cc"));
		System.out.println("existsDire = " + exists);
		exists = fs.exists(new Path("/ee"));
		System.out.println("existsDire = " + exists);
		// create创建文件，如果存在同名不会报错，但是会覆盖掉原文件
		// 所以在创建文件之前，最好判断文件是否存在
		// 默认创建出的文件的权限是-rw-r--r--
		// 如果父目录不存在，那么也会创建一个父目录的
		fsOut = fs.create(new Path("/cc/a.txt"));
		// 可以向文件中写入内容
		fsOut.write("hello hadoop!".getBytes());
		// 内容追加
		fsOut.writeBytes("append content or rewrite content?");
		fsOut.close();
		// 这个方法也是创建文件的，不过也可以创建文件夹，如果不存在的话
		// 同时要定义文件的权限，这里定义为-rw-rw-rw-
		// 同上可以向文件中写入内容
		fsOut = FileSystem.create(
				fs, new Path("/tt/a.txt"),
				new FsPermission(FsAction.READ_WRITE, FsAction.READ_WRITE, FsAction.READ_WRITE)
		);
		fsOut.close();
		// 第二额参数就是是否覆盖已有文件，如果选择了true就跟create(Path path)没什么区别
		// 如果不存在就创建新的文件，如果存在会抛异常FileAlreadyExistsException，
		// 并不是直接返回一个FSDataOutputStream对象
		fsOut = fs.create(new Path("/cc/a.txt"), false);
		fsOut.writeBytes("new fsOut");
		fsOut.close();

		// 新增参数三，手动设置缓冲区大小。4096经验值
		fsOut = fs.create(new Path("/cc/a.txt"), false, 4096);
		fsOut.writeBytes("new fsOut");
		fsOut.close();

		// 参数二，在文件系统中存在的数量，3，代表总共有3份
		fsOut = fs.create(new Path("/cc/a.txt"), (short) 3);
		fsOut.writeBytes("new fsOut");
		fsOut.close();

		// 参数较多，直接在后面加注解，同上的就没必要了
		fsOut = fs.create(
				new Path("/cc/d.txt"),
				new FsPermission(FsAction.READ_WRITE, FsAction.READ_WRITE, FsAction.READ_WRITE),// 权限设置好像不管用
				false,// 是否复写
				4096,// 缓冲区大小
				(short) 3,// 文本个数
				M_100,// blocksize,也就是hdfs块的大小,可以手动设置
				new TaskAttemptContextImpl(new Configuration(), new TaskAttemptID())// 这个参数暂时不太了解
		);
		fsOut.writeBytes("hello hadoop ");
		fsOut.close();
	}

	/**
	 * 删除文件目录
	 * @throws Exception
	 */
	@Test
	public void testDel() throws Exception {
		// 第二个参数代表是否递归删除
		// 如果是多级目录，那么选择false会抛出异常PathIsNotEmptyDirectoryException
		boolean result = fs.delete(new Path("/mk1"), false);
		System.out.println(result);
	}

	/**
	 * 判断当前是否有对文件的特定的权限操作
	 * @throws Exception
	 */
	@Test
	public void testAccess() throws Exception {
		fs.access(new Path("/cc/a.txt"), FsAction.READ_WRITE);
	}

	/**
	 * 为已有文件追加内容
	 * @throws Exception
	 */
	@Test
	public void testAppend() throws Exception {
		FSDataOutputStream fsOut = null;
		// 这里因为是使用的伪分布式，也就是一台机器，
		// 所以在配置文件hdfs-site.xml中设置了参数dfs.replication为1
		// 但是他的dfs.replication仍然是3，所以这里必须手动设置一下才能正常调用append
		// 这个可以参考https://stackoverflow.com/questions/24262362/hadoop-2-2-0-recoveryinprogressexception-while-appending-content-to-an-existing
		fs.setReplication(new Path("/cc/a.txt"), (short) 1);
		// 获取已存在文件的输出流，文件不存在，抛异常FileNotFoundException
		fsOut = fs.append(new Path("/cc/a.txt"));
		fsOut.writeBytes("new fsOut");
		fsOut.close();

		// 可以设置缓冲区大小，不过这个竟然不用设置replication的值，
		// 也及时调用fs.setReplication(new Path("/cc/a.txt"), (short) 1)，
		// 这个和上面的底层都是调用的同一个方法，有点费解
		fsOut = fs.append(new Path("/cc/a.txt"), 4096);
		fsOut.writeBytes(" @append path and buffer@ ");
		fsOut.close();

		// 这里就相当于可以自定义好多属性，如blocksize，buffersize，创建文件还是追加
		FSDataOutputStreamBuilder fsOutBuilder = fs.appendFile(new Path("/cc/c/c.txt"));
		fsOutBuilder.blockSize(M_100);
		// 标记符，调用该方法后如果文件不存在会创建新文件
		fsOutBuilder.create();
		// 标志符，调用之后可以递归，否则FileNotFoundException
		fsOutBuilder.recursive();
		// 标记符，挑用之后会在已存在文件中追加，如果不存在那么抛异常FileNotFoundException
		fsOutBuilder.append();
		fsOutBuilder.bufferSize(4096);
		// 是否复写文件，如果设置为false，并且标记符选择create，且文件存在，则抛出异常FileAlreadyExistsException
		fsOutBuilder.overwrite(true);
		// 这个权限设置也不对，创建的文件是默认权限不是这里定义的
		fsOutBuilder.permission(new FsPermission(FsAction.READ_WRITE, FsAction.READ_WRITE, FsAction.READ_WRITE));
		// 存储的总份数
		fsOutBuilder.replication((short) 1);
		fsOut = fsOutBuilder.build();
		fsOut.writeBytes(" @append path and buffer@@ ");
		fsOut.close();
	}

	/**
	 * 测试上传下载
	 */
	@Test
	public void testUploadAndDown() throws IOException {
		// ----文件上传----
		// 上传文件,如果文件已经存在则覆盖，参数一设置为true会移除本地文件
//		fs.copyFromLocalFile(true, new Path("e://123.txt"), new Path("/e.txt"));
		// 上传多文件到文件服务器，那么最后一个参数应该是上传的根路径，参数一同上，参数二是否复写
//		fs.copyFromLocalFile(true, true, new Path[]{new Path("e://123.txt"), new Path("e://456.txt")}, new Path("/"));

		// 文件上传的其他形式
//		InputStream in = new FileInputStream("e://8848.txt");
//		FSDataOutputStream out = fs.create(new Path("/d.txt"));
//		IOUtils.copyBytes(in, out, 4096, true);

		// ----文件下载----
		// 文件下载的时候需要在本地安装hadoop否则会报错HADOOP_HOME and hadoop.home.dir are unset
		// 但是能在网上进行查找的时候，发现别人说的是在进行操作“上传、下载、创建、删除等”时，都会出现这个异常，
		// 别人的版本是2.8.4，我的版本是2.9.1，可能是版本问题，这里没有测试2.8.4版本的
		// 我这里只有文件下载的时候需要进行设置才会出现这个异常，
		// 所以考虑了下，这个文件下载难道是从一个hdfs，下载到另一个hdfs，
		// 但是呢，参照linux的copyToLocal命令，会下载到本地的linux文件系统，而非hdfs。
		// 解决方案可以参考https://www.jianshu.com/p/93d53d8c64d3
//		fs.copyToLocalFile(new Path("/123.txt"), new Path("e://111.txt"));
		// 这个方法底层源码就是调用的copyFromLocalFile，只不过将第一个参数设置为true
//		fs.moveFromLocalFile(new Path[]{new Path("e://123.txt"), new Path("e://456.txt")}, new Path("/"));

		// 文件下载的其他版本这种方式不存在上述问题，可以直接进行下载了
		InputStream in = fs.open(new Path("/d.txt"));
		FileOutputStream out = new FileOutputStream("e://88481.txt");
		IOUtils.copyBytes(in, out, 4096, true);
	}

}