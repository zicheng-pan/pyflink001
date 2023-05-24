package com.flink.stream.transformation;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class TestMyMap {
    public static void main(String[] args) throws Exception {


        //获取flink的执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //模拟测试数据
        List<String> list = new ArrayList<String>();
        list.add("I love Beijing");
        list.add("I love China");
        list.add("Beijing is the capital of China");


        list.add("I love Beijing");
        list.add("I love China");
        list.add("Beijing is the capital of China");
        //添加批处理的数据源
        DataSet<String> source = env.fromCollection(list);
        source.map(key->key).setParallelism(2).print();
//        //使用flink的map算子来执行离线计算
//        source.map(new MapFunction<String, List<String>>() {
//
//                    public List<String> map(String line) throws Exception {
//                        String[] words = line.split(" ");
//                        List<String> wds = new ArrayList<String>();
//                        for (String word : words) {
//                            wds.add(word);
//                        }
//                        return wds;
//                    }
//                }).
//
//                print();
//        System.out.println("*************************************");
//        //执行flink的flatMap算子
//        source.flatMap(new FlatMapFunction<String, String>() {
//
//                    @Override
//                    public void flatMap(String line, org.apache.flink.util.Collector<String> collector) throws Exception {
//                        String[] words = line.split(" ");
//                        for (String word : words
//                        ) {
//                            collector.collect(word);
//                        }
//                    }
//
//                }).
//
//                print();
//        System.out.println("*************************************");
        //执行flink的mapPartition算子
//        source.mapPartition(new MapPartitionFunction<String, String>() {
//                    Integer index = 0;
//
//                    public void mapPartition(Iterable<String> iterable, Collector<String> collector) throws Exception {
//                        Iterator<String> iterator = iterable.iterator();
//                        while (iterator.hasNext()) {
//                            String line = iterator.next();
//                            String[] words = line.split(" ");
//                            for (String word : words
//                            ) {
//                                collector.collect("分区" + index + ",单词为：" + word);
//                            }
//                        }
//                        index++;
//                    }
//                }).partitionByHash(key->key).setParallelism(2)
//                .print();


        env.execute("FlinkDemo1");
    }
}
