package com.learn.bigdata.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;

/**
 * use API to develop
 * more professional (using junit)
 */
public class HDFSApp {
    public static final String HDFS_PATH = "hdfs://localhost:8020";

    //junit test
    FileSystem fileSystem = null;
    Configuration configuration = null;

    @Before
    public void setUp() throws Exception {
        System.out.println("-----set up-----");

        configuration = new Configuration();
        configuration.set("dfs.replication", "1");
        fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration, "ada");
    }

    @Test
    public void makeDir() throws Exception{
        // note all the operations is related to the "fileSystem"
        fileSystem.mkdirs(new Path("/hdfsapi/test"));
    }

    @Test
    public void text() throws Exception{
        FSDataInputStream in = fileSystem.open(new Path("/cdh_version.properties"));
        IOUtils.copyBytes(in, System.out, 1024);
    }

    @Test
    public void createFile () throws Exception {
        FSDataOutputStream out = fileSystem.create(new Path("/hdfsapi/test/b.txt"));
        out.writeUTF("awesome");
        out.flush();
        out.close();
    }

    @Test
    public void testReplication(){
        System.out.println(configuration.get("dfs.replication"));
    }

    @Test
    public void reName() throws IOException {
        Path src = new Path("/hdfsapi/test/b.txt");
        Path dst = new Path("/hdfsapi/test/b-rename.txt");
        fileSystem.rename(src, dst);
    }

    @Test
    public void copyFromLocal() throws IOException {
        Path src = new Path("/home/ada/Downloads/spark-2.3.0.tgz");
        Path dst = new Path("/hdfsapi/test/");
        fileSystem.copyFromLocalFile(src, dst);
    }

    @Test
    public void copyLargeFileFromLocalWithProgress() throws IOException {
        Path src = new Path("/home/ada/Downloads/jdk-8u201-linux-x64.tar.gz");  // 183Mb
        Path dst = new Path("/hdfsapi/test/jdk.tgz");

        InputStream in = new BufferedInputStream(
                new FileInputStream(
                        new File("/home/ada/Downloads/jdk-8u201-linux-x64.tar.gz")));

        FSDataOutputStream out = fileSystem.create(dst, new Progressable() {
            @Override
            public void progress() {
                System.out.print("=");
            }
        });

        IOUtils.copyBytes(in, out, 4096);
    }

    @Test
    public void copyToLocal() throws IOException {
        Path src = new Path("/hdfsapi/test/a.txt");
        Path dst = new Path("/home/ada/");
        fileSystem.copyToLocalFile(src, dst);
    }

    @Test
    public void listFiles() throws IOException {
        FileStatus[] statuses = fileSystem.listStatus(new Path("/hdfsapi/test"));

        for(FileStatus file : statuses) {
            String isDir = file.isDirectory() ? "file" : "folder";
            String permission = file.getPermission().toString();
            short replication = file.getReplication();
            long length = file.getLen();
            String owner = file.getOwner();
            String path = file.getPath().toString();

            System.out.println(isDir + "\t" + permission + "\t" +
                    owner + "\t" + replication + "\t" +
                    length + "\t" + path + "\t" );
        }
    }

    @Test
    public void listFilesRecursive() throws IOException {
        RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(new Path("/hdfsapi/test"), true);

        while (files.hasNext()) {
            LocatedFileStatus file = files.next();
            String isDir = file.isDirectory() ? "file" : "folder";
            String permission = file.getPermission().toString();
            short replication = file.getReplication();
            long length = file.getLen();
            String owner = file.getOwner();
            String path = file.getPath().toString();

            System.out.println(isDir + "\t" + permission + "\t" +
                    owner + "\t" + replication + "\t" +
                    length + "\t" + path + "\t" );
        }
    }

    @Test
    public void getFileBlockLocations() throws IOException {
        FileStatus fileStatus = fileSystem.getFileStatus(new Path("/hdfsapi/test/jdk.tgz"));
        BlockLocation[] blockLocations = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());

        for (BlockLocation block : blockLocations) {
            for (String name : block.getNames()) {
                System.out.println(name + " : " + block.getLength() + " : " + block.getOffset());
            }
        }
    }

    @Test
    public void delete() throws Exception {
        boolean result = fileSystem.delete(new Path("/hdfsapi/test/jdk.tgz"), true);
        System.out.println(result);
    }

    @After
    public void tearDown() throws Exception {
        configuration = null;
        fileSystem = null;

        System.out.println("-----tear down-----");
    }


//    public static void main(String[] args) throws Exception{
//
//        Configuration configuration = new Configuration();
//        FileSystem fileSystem = FileSystem.get(new URI("hdfs://localhost:8020"), configuration, "ada");
//
//        Path path = new Path("/hdfsapi/test");
//
//        boolean result = fileSystem.mkdirs(path);
//        System.out.println(result);
//    }
}
