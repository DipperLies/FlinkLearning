package org.myorg.myFlink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author Michael
 * @date 2019-08-29 13:33
 */
public class SocketWindowWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> text = env.socketTextStream("10.108.240.139", 9006);

        DataStream<Tuple2<String, Integer>> wordCounts = text
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) {
                        for (String word : value.split("\\s")) {
                            collector.collect(Tuple2.of(word, 1));
                        }
                    }
                });

        DataStream<Tuple2<String,Integer>> wordcount =wordCounts
                .keyBy(0)
                .timeWindow(Time.seconds(5))
                .sum(1);

        wordcount.print().setParallelism(1);
        env.execute("Socket Window Wordcount");
    }

}
