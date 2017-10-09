package com.lt.hadoop.step1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by taoshiliu on 2017/10/8.
 */
public class Mapper1 extends Mapper<LongWritable,Text,Text,Text> {

    private Text outKey = new Text();
    private Text outvalue = new Text();

    /**
     *key:1
     *value:1       1_0,2_3,3_-1,4_2,5_-3
     */
    protected void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException {

        String[] rowAndLine = value.toString().split("\t");
        //矩阵的行号
        String row = rowAndLine[0];
        String[] lines = rowAndLine[1].split(",");

        for(int i = 0;i < lines.length;i++) {
            String column = lines[i].split("_")[0];
            String valueStr = lines[i].split("_")[1];

            //封装输出（T）
            outKey.set(column);
            outvalue.set(row + "_" +valueStr);
            context.write(outKey,outvalue);
        }
    }
}
