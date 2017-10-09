package com.lt.hadoop.step3;

import com.lt.hadoop.step2.Mapper2;
import com.lt.hadoop.step2.Reducer2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by taoshiliu on 2017/10/8.
 */
public class MR3 {
    //输入路径
    private static String inPath = "/ContentCF/step1_input/ItemProfile.txt";
    //输出路径
    private static String outPath = "/ContentCF/step3_output";
    //step1的输出作为全局缓存
    private static String cache = "/ContentCF/step2_output/part-r-00000";
    //hdfs地址
    private static String hdfs = "hdfs://localhost:9000";

    public int run() {
        try {
            //创建job配置
            Configuration conf = new Configuration();
            //设置hdfs的地址
            conf.set("fs,defaultFS",hdfs);
            //创建一个JOB实例
            Job job = Job.getInstance(conf,"step3");
            //添加分布式缓存文件（添加全局缓存）
            job.addCacheArchive(new URI(cache + "#itemUserScore"));

            job.setJarByClass(com.lt.hadoop.step2.MR2.class);
            job.setMapperClass(Mapper2.class);
            job.setReducerClass(Reducer2.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            //设置输入输出的路径
            FileSystem fs = FileSystem.get(conf);
            Path inputPath = new Path(inPath);
            if(fs.exists(inputPath)) {
                FileInputFormat.addInputPath(job,inputPath);
            }

            Path outputPath = new Path(outPath);
            fs.delete(outputPath,true);

            FileOutputFormat.setOutputPath(job,outputPath);

            return job.waitForCompletion(true)?1:-1;

        } catch (IOException e) {
            e.printStackTrace();
        }catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }catch (URISyntaxException e) {
            e.printStackTrace();
        }

        return -1;
    }

    public static void main(String[] args) {
        int result = -1;
        result = new com.lt.hadoop.step2.MR2().run();
        if(result == 1) {
            System.out.println("step3运行成功");
        }else if(result == -1) {
            System.out.println("step3运行失败");
        }
    }
}
