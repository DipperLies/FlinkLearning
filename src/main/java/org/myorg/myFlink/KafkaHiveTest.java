package org.myorg.myFlink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.fs.StringWriter;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;


import java.time.ZoneId;
import java.util.Properties;

/**
 * @author Michael
 * @date 2019-09-05 16:11
 */
public class KafkaHiveTest {
    public static void main(String[] args ) throws Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties properties=new Properties();
        properties.setProperty("bootstrap.servers","10.108.240.137:9092,10.108.240.147:9092,10.108.240.157:9092");
        properties.setProperty("zookeeper.connect","10.108.240.137:2181,10.108.240.147:2181,10.108.240.157:2181");
        properties.setProperty("group.id","test008");

        FlinkKafkaConsumer08<String> myConsumer =new FlinkKafkaConsumer08<String>("P2OEEMDB.EQP_STATE_MPA",new SimpleStringSchema(),
                properties);
        DataStream<String> stream = env.addSource(myConsumer);
//        DataStream<Tuple2<String,Integer>> counts=stream.flatMap(new LineSplitter()).keyBy(0).sum(1);

        stream.print();

        System.out.println("*********** hdfs ***********************");
        BucketingSink<String> sink = new BucketingSink<String>("hdfs://10.108.7.181:8020/tmp/path");
        sink.setBucketer(new DateTimeBucketer<>("yyyy-MM-dd--HHmm", ZoneId.of("Asia/Shanghai")));
        sink.setWriter(new StringWriter<>());
        sink.setBatchSize(1024 * 1024 * 400 ); // this is 400 MB,
        sink.setBatchRolloverInterval(10 * 60 * 1000); // this is 20 mins

        stream.addSink(sink);
        env.execute("Word Count Kafka Test");

    }

    public static final class LineSplitter implements FlatMapFunction<String,Tuple2<String,Integer>>{
        private static final long serialVersionUID = 1L;

        @Override
        public void flatMap(String value, Collector<Tuple2<String,Integer>> out){
//            String[] tokens=value.toLowerCase().split("\\W+");
            String[] tokens=value.toLowerCase().split("\\\\N+");
            for(String token:tokens){
                if(token.length()>0){
                    out.collect(new Tuple2<String,Integer>(token,1));
                }
            }
        }
    }

}
