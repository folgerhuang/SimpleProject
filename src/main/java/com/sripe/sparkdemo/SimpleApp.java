package com.sripe.sparkdemo;/* com.sripe.sparkdemo.SimpleApp.java */

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class SimpleApp {
    public static void main(String[] args) {
        SparkConf config = new SparkConf()
                .setMaster("spark://192.168.119.133:7077")
                .setAppName("SparkDemo")
                //用PackageJars 打包
                .setJars(new String[]{"G:\\AndroidStudioProjects\\SimpleProject\\com.sripe.sparkdemo.jar"});
        JavaSparkContext sc = new JavaSparkContext(config);

        String file = "hdfs://learnhadoopnode:9000/user/shiyu/input/yarn-site.xml"; //对hdfs中的文件操作
        JavaRDD<String> data = sc.textFile(file, 4).cache();

        JavaRDD<String> words = data.flatMap(new FlatMapFunction<String, String>() {
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        });

        long numAs = words.filter(new Function<String, Boolean>() {
            public Boolean call(String v1) throws Exception {
                return v1.contains("under");
            }
        }).count();

        long numBs = words.filter(new Function<String, Boolean>() {
            public Boolean call(String v1) throws Exception {
                return v1.contains("b");
            }
        }).count();
        System.out.println("Lines with under:" + numAs + ", lines with b:" + numBs);


        //统计字数wordcount
        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> counts = stringIntegerJavaPairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }).sortByKey();//排序

        List<Tuple2<String, Integer>> collect = counts.collect();

        for (Tuple2<?, ?> item : collect) {
            System.out.printf("(%s,%d)\n", item._1, item._2);
        }
    }
}