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
                .setMaster("spark://")
                .setAppName("WordCount");

//        System.setProperty("hadoop.home.dir", "D:\\jiabao\\hadoop-2.7.3");

        JavaSparkContext context = new JavaSparkContext(conf);
        JavaRDD<String> lines = context.textFile("C:\\Users\\於建\\Desktop\\WordCount.txt");

        JavaPairRDD<String,Integer> pairRDD = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(word->new Tuple2<String, Integer>(word,1))
                .reduceByKey((x,y)->(x+y));

        pairRDD.foreach(tup->System.out.println(tup._1+" appears "+tup._2()+" times"));

        context.close();
    }
}
