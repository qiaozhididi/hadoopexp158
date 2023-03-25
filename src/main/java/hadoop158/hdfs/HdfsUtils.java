package hadoop158.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

public class HdfsUtils {
    /**
     * 在HDFS上创建文件夹并写入内容
     *
     * @param conf         HDFS的配置
     * @param hdfsFilePath HDFS创建的文件路径
     * @param content      写入文件的内容
     * @param overwrite    true:如果文件存在则覆盖，false:如果文件存在则不覆盖
     */
    public static boolean createFile(Configuration conf, URI uri, String hdfsFilePath, String content, boolean overwrite) {
        FileSystem fs = null;
        FSDataOutputStream os = null;
        boolean rs = false;
        try {
            //指定用户名，获取FileSystem对象
            fs = FileSystem.get(uri, conf, "hadoop");
            //定义文件路径
            Path dfs = new Path(hdfsFilePath);
            os = fs.create(dfs, overwrite);
            //写入内容
            os.write(content.getBytes());
            //成功创建文件，设置为true
            rs = true;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (os != null) {
                    os.close();
                }
                if (fs != null) {
                    //关闭FileSystem对象
                    fs.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return rs;
    }
    //新增uploadFile方法
    public static boolean uploadFile(Configuration conf, URI uri, String localFilePath, String hdfsFilePath, boolean overwrite) {
        FileSystem fs = null;
        boolean rs = false;
        try {
            //指定用户名，获取FileSystem对象
            fs = FileSystem.get(uri, conf, "hadoop");
            //定义文件路径
            Path dfs = new Path(hdfsFilePath);
            Path local = new Path(localFilePath);
            //上传文件
            fs.copyFromLocalFile(local, dfs);
            //成功创建文件，设置为true
            rs = true;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (fs != null) {
                    //关闭FileSystem对象
                    fs.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return rs;
    }
    //新增downloadFile方法
    public static boolean downloadFile(Configuration conf, URI uri, String localFilePath, String hdfsFilePath, boolean overwrite) {
        FileSystem fs = null;
        boolean rs = false;
        try {
            //指定用户名，获取FileSystem对象
            fs = FileSystem.get(uri, conf, "hadoop");
            //定义文件路径
            Path dfs = new Path(hdfsFilePath);
            Path local = new Path(localFilePath);
            //下载文件
            fs.copyToLocalFile(dfs, local);
            //成功创建文件，设置为true
            rs = true;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (fs != null) {
                    //关闭FileSystem对象
                    fs.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return rs;
    }
}
