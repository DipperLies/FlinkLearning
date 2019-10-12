package org.myorg.myFlink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.sun.istack.Nullable;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;

import org.myorg.myFlink.Pojo.Prti;
import java.time.ZoneId;
import java.util.Properties;

/**
 * @author Michael
 * @date 2019-10-10 13:45
 */
public class FlinkReadKafkaToHdfs {
    private final static StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

    private final static Properties properties = new Properties();

    private static void init() {
        environment.enableCheckpointing(5000);
        environment.setParallelism(1);
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //kafka的节点的IP或者hostName，多个使用逗号分隔
        properties.setProperty("bootstrap.servers","10.108.240.137:9092,10.108.240.147:9092,10.108.240.157:9092");
        properties.setProperty("zookeeper.connect","10.108.240.137:2181,10.108.240.147:2181,10.108.240.157:2181");
        //only required for Kafka 0.8;
//        properties.setProperty("zookeeper.connect", "192.168.0.10:2181");
        //flink consumer flink的消费者的group.id
        properties.setProperty("group.id","test008");
        //第一种方式：路径写自己代码上的路径
//        properties.setProperty("fs.hdfs.hadoopconf", "...\\src\\main\\resources");
        //第二种方式：填写一个schema参数即可
//        properties.setProperty("fs.default-scheme", "hdfs://10.108.7.181:8020");

        properties.setProperty("kafka.topic", "P2OEEMDB.EQP_STATE_MPA");
        properties.setProperty("hfds.path", "hdfs://10.108.7.181:8020/tmp/path");
        properties.setProperty("hdfs.path.date.format", "yyyy-MM-dd");
        properties.setProperty("hdfs.path.date.zone", "Asia/Shanghai");
        properties.setProperty("window.time.second", "60");
    }

    public static void readKafkaToHdfsByReflect(StreamExecutionEnvironment environment, Properties properties) throws Exception {
        String topic = properties.getProperty("kafka.topic");
        String path = properties.getProperty("hfds.path");
        String pathFormat = properties.getProperty("hdfs.path.date.format");
        String zone = properties.getProperty("hdfs.path.date.zone");
        Long windowTime = Long.valueOf(properties.getProperty("window.time.second"));
        FlinkKafkaConsumer010<String> flinkKafkaConsumer010 = new FlinkKafkaConsumer010<>(topic, new SimpleStringSchema(), properties);
        KeyedStream<Prti, String> KeyedStream = environment.addSource(flinkKafkaConsumer010)
                .map(FlinkReadKafkaToHdfs::transformData)
                .assignTimestampsAndWatermarks(new CustomWatermarks<Prti>())
                .keyBy(Prti::getPlateNo);

        DataStream<Prti> output = KeyedStream.window(TumblingEventTimeWindows.of(Time.seconds(windowTime)))
                .apply(new WindowFunction<Prti, Prti, String, TimeWindow>() {
                    @Override
                    public void apply(String key, TimeWindow timeWindow, Iterable<Prti> iterable, Collector<Prti> collector) throws Exception {
                        System.out.println("keyBy: " + key + ", window: " + timeWindow.toString());
                        iterable.forEach(collector::collect);
                    }
                });
        //写入HDFS，parquet格式
        System.out.println("*********** hdfs ***********************");
        DateTimeBucketAssigner<Prti> bucketAssigner = new DateTimeBucketAssigner<>(pathFormat, ZoneId.of(zone));
        StreamingFileSink<Prti> streamingFileSink = StreamingFileSink.
                forBulkFormat(new Path(path), ParquetAvroWriters.forReflectRecord(Prti.class))
                .withBucketAssigner(bucketAssigner)
                .build();
        output.addSink(streamingFileSink).name("Hdfs Sink");
        environment.execute("PrtiData");
    }

    private static Prti transformData(String data) throws Exception {
        if (data != null && !data.isEmpty()) {
            JSONObject value = JSON.parseObject(data);
            Prti prti = new Prti();
            prti.setPlateNo(value.getString("RSD_02"));
            prti.setPassingTime(value.getString("op_ts"));
            if (!value.containsKey("RAWID") || !(value.getString("RAWID") instanceof String)) {
                prti.setPlateNo("Null");
            } else {
                prti.setPlateNo(value.getString("RAWID"));
            }
            if (!value.containsKey("pos")|| !(value.getString("pos") instanceof String)) {
                prti.setPassingTime("0L");
            } else {
                prti.setPassingTime(value.getString("pos"));
            }
            System.out.println(prti.toString());
            return prti;
        } else {
            return new Prti();
        }
    }

    private static class CustomWatermarks<T> implements AssignerWithPunctuatedWatermarks<Prti> {

        private Long cuurentTime = 0L;

        @Nullable
        @Override
        public Watermark checkAndGetNextWatermark(Prti prti, long l) {
            return new Watermark(cuurentTime);
        }

        @Override
        public long extractTimestamp(Prti prti, long l) {
            Long passingTime = Long.valueOf(prti.getPassingTime());
            cuurentTime = Math.max(passingTime, cuurentTime);
            return passingTime;
        }
    }

    public static void main(String[] args) throws Exception {
        init();
        readKafkaToHdfsByReflect(environment, properties);
    }
}
