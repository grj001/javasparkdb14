package com.zhiyou.javasparkdb14;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class WordCount {

	public static void main(String[] args){
		
		SparkConf conf = new SparkConf()
				.setMaster("local[*]")
				.setAppName("java spark wc");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//构建rdd
		JavaRDD<String> lineRdd = sc.textFile("hdfs:///user/user-logs-large.txt", 2);
		
		//把一行转换成单词
		JavaRDD<String> wordRdd = lineRdd.flatMap(new FlatMapFunction<String,String>() {
			@Override
			public Iterator<String> call(String t) throws Exception {
				return Arrays.asList(t.split("\\s")).iterator();
			}
	
		});
		
		//把单词的rdd转换成, kv的rdd
		JavaPairRDD<String, Integer> javaPairRDD = 
				wordRdd.mapToPair(
						new PairFunction<String, String, Integer>() {

							@Override
							public Tuple2<String, Integer> call(String t) throws Exception {
								return new Tuple2<String,Integer>(t,1);
							}
							
				});
		JavaPairRDD<String, Integer> result = javaPairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1+v2;
			}
		});
		result.saveAsTextFile("/user/java-spark-output");
		sc.stop();
	}

}
