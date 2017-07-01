package org.apache.spark.examples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by yujian on 2017/6/20.
 */
public class Word_Count {
    public static void main(String[] args){
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("WordCount");

        JavaSparkContext context = new JavaSparkContext(conf);
        JavaRDD<String> lines = context.textFile("E:\\WordCount.txt");

        JavaPairRDD<String,Integer> pairRDD = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(word->new Tuple2<String, Integer>(word,1))
                //其实在RDD中是没有reduceBykey这个方法的，会触发Scala的隐式转换
                //会在RDD中找到rddToPairRDDFunctions的隐式转换，将其转换为PairRDDFunctions，然后调用PairRDDFunctions中的reduceBykey方法
                .reduceByKey((x,y)->(x+y));

        pairRDD.foreach(tup->System.out.println(tup._1+" appears "+tup._2()+" times"));

        context.close();
    }
}
