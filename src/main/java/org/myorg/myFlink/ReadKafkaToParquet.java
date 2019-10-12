package org.myorg.myFlink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.sun.istack.Nullable;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
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
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.myorg.myFlink.Pojo.TopicSource;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.util.Properties;

/**
 * @author Michael
 * @date 2019-10-12 14:43
 */
public class ReadKafkaToParquet {
    private final static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    private final static Properties props = new Properties();

    static {
        /** Set flink env info. */
        env.enableCheckpointing(10 * 1000);
        env.setParallelism(1);
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        /** Set kafka broker info. */
        props.setProperty("bootstrap.servers", "10.108.240.137:9092,10.108.240.147:9092,10.108.240.157:9092");
        props.setProperty("zookeeper.connect", "10.108.240.137:2181,10.108.240.147:2181,10.108.240.157:2181");
        props.setProperty("group.id", "test008");
        props.setProperty("kafka.topic", "P2OEEMDB.EQP_STATE_MPA");

        /** Set hdfs info. */
        props.setProperty("hdfs.path", "hdfs://10.108.7.181:8020/tmp/path");
        props.setProperty("hdfs.path.date.format", "yyyy-MM-dd--HHmm");
        props.setProperty("hdfs.path.date.zone", "Asia/Shanghai");
        props.setProperty("window.time.second", "10");

    }

    /**
     * Consumer topic data && parse to hdfs.
     */
    public static void getTopicToHdfsByParquet(StreamExecutionEnvironment env, Properties props) {
        try {

            String topic = props.getProperty("kafka.topic");
            String path = props.getProperty("hdfs.path");
            String pathFormat = props.getProperty("hdfs.path.date.format");
            String zone = props.getProperty("hdfs.path.date.zone");
            Long windowTime = Long.valueOf(props.getProperty("window.time.second"));
            FlinkKafkaConsumer010<String> flinkKafkaConsumer010 = new FlinkKafkaConsumer010<>(topic, new SimpleStringSchema(), props);
            KeyedStream<TopicSource, String> KeyedStream = env.addSource(flinkKafkaConsumer010).map(ReadKafkaToParquet::transformData).assignTimestampsAndWatermarks(new ReadKafkaToParquet.CustomWatermarks<TopicSource>()).keyBy(TopicSource::getId);

            DataStream<TopicSource> output = KeyedStream.window(TumblingEventTimeWindows.of(Time.seconds(windowTime))).apply(new WindowFunction<TopicSource, TopicSource, String, TimeWindow>() {

                private static final long serialVersionUID = 1L;

                @Override
                public void apply(String key, TimeWindow timeWindow, Iterable<TopicSource> iterable, Collector<TopicSource> collector) throws Exception {
                    System.out.println("@@@@@@@keyBy: " + key + ", window: " + timeWindow.toString());
                    iterable.forEach(collector::collect);
                }
            });

            output.print();
            // Send hdfs by parquet
            System.out.println("*********** hdfs ***********************");
            DateTimeBucketAssigner<TopicSource> bucketAssigner = new DateTimeBucketAssigner<>(pathFormat, ZoneId.of(zone));
            final StreamingFileSink<TopicSource> streamingFileSink = StreamingFileSink.forBulkFormat(new Path(path)
                    , ParquetAvroWriters.forReflectRecord(TopicSource.class))
                    .withBucketAssigner(bucketAssigner)
                    .build();
//            BucketingSink<TopicSource> sink = new BucketingSink<TopicSource>("hdfs://10.108.7.181:8020/tmp/path");
//            sink.setBucketer(new DateTimeBucketer<>("yyyy-MM-dd--HHmm", ZoneId.of("Asia/Shanghai")));
//            sink.setBatchSize(1024 * 1024 * 400); // this is 400 MB,
//            sink.setBatchRolloverInterval(1 * 60 * 1000); // this is 20 mins


            output.addSink(streamingFileSink).name("Sink To HDFS");
            env.execute("TopicData");
        } catch (Exception ex) {
            System.out.println("!!####!!Exception");
            ex.printStackTrace();
        }
    }

    private static TopicSource transformData(String data) {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        if (data != null && !data.isEmpty()) {
            JSONObject value = JSON.parseObject(data.replaceAll(":null,", ":\"null\","));
            System.out.println("###" + value.toString());
            TopicSource topic = new TopicSource();
            try {
                if (!value.containsKey("RAWID") || !(value.getString("RAWID") instanceof String)) {
                    topic.setId("Null");
                } else {
                    topic.setId(value.getString("RAWID"));
                }
                if (!value.containsKey("pos")|| !(value.getString("pos") instanceof String)) {
                    topic.setTime("0");
                } else {
                    topic.setTime(value.getString("pos"));
                }
                if ((!value.containsKey("RSD_02")) || !(value.getString("RSD_02") instanceof String)) {
                    topic.setEqp_id("Null");
                } else {
                    topic.setEqp_id(value.getString("RSD_02"));
                }
                if ((!value.containsKey("OLD_STATE")) || !(value.getString("OLD_STATE") instanceof String)) {
                    topic.setOld_state("Null");
                } else {
                    topic.setOld_state(value.getString("OLD_STATE"));
                }
                if ((!value.containsKey("NEW_STATE")) || !(value.getString("NEW_STATE") instanceof String)) {
                    topic.setNew_state("Null");
                } else {
                    topic.setNew_state(value.getString("NEW_STATE"));
                }
                System.out.println(topic.toString());
            } catch (Exception e) {
                System.out.println("!!!!!!Exception");
                e.printStackTrace();
            }
            return topic;
        } else {
            return new TopicSource();
        }
    }

    private static class CustomWatermarks<T> implements AssignerWithPunctuatedWatermarks<TopicSource> {

        /**
         *
         */
        private static final long serialVersionUID = 1L;
        private Long cuurentTime = 0L;
        final Long maxOutOfOrderness = 10000L;// 最大允许的乱序时间是10s

        @Nullable
        @Override
        public Watermark checkAndGetNextWatermark(TopicSource topic, long l) {
            return new Watermark(cuurentTime);
        }

        @Override
        public long extractTimestamp(TopicSource topic, long l) {
            Long time = Long.parseLong(topic.getTime());
            cuurentTime = Math.max(time, cuurentTime);
            System.out.println("#### currentTime:" + cuurentTime);
            System.out.println("#### time:" + time);
            return time;
        }
    }

    public static void main(String[] args) {
        getTopicToHdfsByParquet(env, props);
    }
}
