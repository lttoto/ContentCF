package com.lt.hadoop.step3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by taoshiliu on 2017/10/8.
 */
public class Mapper3 extends Mapper<LongWritable,Text,Text,Text> {

    private Text outKey = new Text();
    private Text outvalue = new Text();

    private List<String> cacheList = new ArrayList<String>();

    private DecimalFormat df = new DecimalFormat("0.00");

    protected void setup(Context context) throws IOException,InterruptedException {
        super.setup(context);
        //通过输入流将全局缓存中的matrix2读入List<String>中
        FileReader fr = new FileReader("itemUserScore");
        BufferedReader br = new BufferedReader(fr);

        String line = null;
        while ((line = br.readLine()) != null) {
            cacheList.add(line);
        }

        fr.close();
        br.close();
    }

    protected void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException {

        String row_matrix1 = value.toString().split("\t")[0];

        String[] column_value_array_matrix1 = value.toString().split("\t")[1].split(",");

        double denominator1 = 0;
        //计算左矩阵行的空间距离
        for(String column_value : column_value_array_matrix1) {
            String score = column_value.split("_")[1];
            denominator1 += Double.valueOf(score) * Double.valueOf(score);
        }
        denominator1 = Math.sqrt(denominator1);

        for(String line : cacheList) {
            String row_matrix2 = line.toString().split("\t")[0];
            String[] column_value_array_matrix2 = line.toString().split("\t")[1].split(",");

            double denominator2 = 0;
            //计算右矩阵行的空间距离
            for(String column_value : column_value_array_matrix2) {
                String score = column_value.split("_")[1];
                denominator2 += Double.valueOf(score) * Double.valueOf(score);
            }
            denominator2 = Math.sqrt(denominator2);

            int numerator = 0;
            for(String column_value_matrix1 : column_value_array_matrix1) {
                String column_matrix1 = column_value_matrix1.split("_")[0];
                String value_matrix1 = column_value_matrix1.split("_")[1];
                for(String column_value_matrix2:column_value_array_matrix2) {
                    if(column_value_matrix2.startsWith(column_matrix1 + "_")) {
                        String value_matrix2 = column_value_matrix2.split("_")[1];

                        numerator += Integer.valueOf(value_matrix1) * Integer.valueOf(value_matrix2);
                    }
                }
            }

            //余弦相似度
            double cos = numerator / (denominator1 * denominator2);
            if(cos == 0) {
                continue;
            }

            outKey.set(row_matrix2);
            outvalue.set(row_matrix1+ "_" + df.format(cos));

            context.write(outKey,outvalue);
        }


    }
}
