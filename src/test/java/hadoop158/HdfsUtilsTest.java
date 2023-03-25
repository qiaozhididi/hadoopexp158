package hadoop158;

import hadoop158.hdfs.HdfsUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;


import java.net.URI;

public class HdfsUtilsTest {
    @Test
    public void testCreate() {
        Configuration conf = new Configuration();
        boolean rs = false;
        //创建的文件路径
        String filePath = "/158/test.txt";
        String content = "hello";
        try {
            URI uri = new URI("hdfs://192.168.128.11:8020");
            rs = HdfsUtils.createFile(conf, uri, filePath, content, true);
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (rs) {
            System.out.println("Create successfully!");
        } else {
            System.out.println("Create fail!");
        }
    }

    //新增testUpload方法
    @Test
    public void testUpload() {
        Configuration conf = new Configuration();
        boolean rs = false;
        //上传的文件路径
        String localFilePath = "D:/iot-dev/hadoopexp158/src/main/resources/test.txt";
        //上传到HDFS的文件路径
        String hdfsFilePath = "/158/testUpload.txt";
        try {
            URI uri = new URI("hdfs://192.168.128.11:8020");
            rs = HdfsUtils.uploadFile(conf, uri, localFilePath, hdfsFilePath, true);
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (rs) {
            System.out.println("Upload successfully!");
        } else {
            System.out.println("Upload fail!");
        }
    }

    //新增testDownload方法
    @Test
    public void testDownload() {
        Configuration conf = new Configuration();
        boolean rs = false;
        //下载的文件路径
        String hdfsFilePath = "/158/testUpload.txt";
        //下载到本地的文件路径
        String localFilePath = "file:/D:/iot-dev/hadoopexp158/src/main/resources/testDownload.txt";
        try {
            URI uri = new URI("hdfs://192.168.128.11:8020");
            rs = HdfsUtils.downloadFile(conf, uri, localFilePath, hdfsFilePath, true);
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (rs) {
            System.out.println("Download successfully!");
        } else {
            System.out.println("Download fail!");
        }
    }
}
