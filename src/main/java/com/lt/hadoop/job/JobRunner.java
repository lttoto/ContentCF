package com.lt.hadoop.job;

import com.lt.hadoop.step1.MR1;
import com.lt.hadoop.step2.MR2;
import com.lt.hadoop.step3.MR3;


/**
 * Created by taoshiliu on 2017/10/8.
 */
public class JobRunner {

    public static void main(String[] args) {
        int status1 = -1;
        int status2 = -1;
        int status3 = -1;


        status1 = new MR1().run();
        if(status1 == 1) {
            System.out.println("Step1运行成功，开始运行Step2");
            status2 = new MR2().run();
        }else {
            System.out.println("Step1运行失败！！！");
        }

        if(status2 == 1) {
            System.out.println("Step2运行成功，开始运行Step2");
            status3 = new MR3().run();
        }else {
            System.out.println("Step2运行失败！！！");
        }

        if(status3 == 1) {
            System.out.println("Step5运行成功，程序结束");
        }else {
            System.out.println("Step5运行失败！！！");
        }
    }

}
