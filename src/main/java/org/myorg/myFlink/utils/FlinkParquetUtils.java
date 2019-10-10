package org.myorg.myFlink.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.sun.istack.Nullable;
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
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.myorg.myFlink.Pojo.TopicSource;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.util.Properties;

/**
 * @author Michael
 * @date 2019-10-10 9:35
 */
public class FlinkParquetUtils {
    private final static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    private final static Properties props = new Properties();

    static {
        /** Set flink env info. */
        env.enableCheckpointing(60 * 1000);
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        /** Set kafka broker info. */
        props.setProperty("bootstrap.servers","10.108.240.137:9092,10.108.240.147:9092,10.108.240.157:9092");
        props.setProperty("zookeeper.connect","10.108.240.137:2181,10.108.240.147:2181,10.108.240.157:2181");
        props.setProperty("group.id","test008");
        props.setProperty("kafka.topic", "P2OEEMDB.EQP_STATE_MPA");

        /** Set hdfs info. */
        props.setProperty("hdfs.path", "hdfs://10.108.7.181:8020/tmp/path");
        props.setProperty("hdfs.path.date.format", "yyyy-MM-dd");
        props.setProperty("hdfs.path.date.zone", "Asia/Shanghai");
        props.setProperty("window.time.second", "60");

    }

    /** Consumer topic data && parse to hdfs. */
    public static void getTopicToHdfsByParquet(StreamExecutionEnvironment env, Properties props) {
        try {
            String topic = props.getProperty("kafka.topic");
            String path = props.getProperty("hdfs.path");
            String pathFormat = props.getProperty("hdfs.path.date.format");
            String zone = props.getProperty("hdfs.path.date.zone");
            Long windowTime = Long.valueOf(props.getProperty("window.time.second"));
            FlinkKafkaConsumer010<String> flinkKafkaConsumer010 = new FlinkKafkaConsumer010<>(topic, new SimpleStringSchema(), props);
            KeyedStream<TopicSource, String> KeyedStream = env.addSource(flinkKafkaConsumer010).map(FlinkParquetUtils::transformData).assignTimestampsAndWatermarks(new CustomWatermarks<TopicSource>()).keyBy(TopicSource::getId);

            DataStream<TopicSource> output = KeyedStream.window(TumblingEventTimeWindows.of(Time.seconds(windowTime))).apply(new WindowFunction<TopicSource, TopicSource, String, TimeWindow>() {
                /**
                 *
                 */
                private static final long serialVersionUID = 1L;

                @Override
                public void apply(String key, TimeWindow timeWindow, Iterable<TopicSource> iterable, Collector<TopicSource> collector) throws Exception {
                    System.out.println("keyBy: " + key + ", window: " + timeWindow.toString());
                    iterable.forEach(collector::collect);
                }
            });

            output.print();
            // Send hdfs by parquet
            System.out.println("*********** hdfs ***********************");
            DateTimeBucketAssigner<TopicSource> bucketAssigner = new DateTimeBucketAssigner<>(pathFormat, ZoneId.of(zone));
            StreamingFileSink<TopicSource> streamingFileSink = StreamingFileSink.forBulkFormat(new Path(path), ParquetAvroWriters.forReflectRecord(TopicSource.class)).withBucketAssigner(bucketAssigner).build();
            output.addSink(streamingFileSink).name("Sink To HDFS");
            env.execute("TopicData");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private static TopicSource transformData(String data) throws Exception {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        if (data != null && !data.isEmpty()) {
            JSONObject value = JSON.parseObject(data);
            System.out.println("###"+value.toString());
            TopicSource topic = new TopicSource();
            topic.setId(value.getString("PRODUCT_ID"));
            topic.setTime(df.parse(value.getString("op_ts")).getTime());
            topic.setEqp_id(value.getString("RSD_02"));
            topic.setOld_state(value.getString("OLD_STATE"));
            topic.setNew_state(value.getString("NEW_STATE"));
            System.out.println(topic.toString());
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

        @Nullable
        @Override
        public Watermark checkAndGetNextWatermark(TopicSource topic, long l) {
            return new Watermark(cuurentTime);
        }

        @Override
        public long extractTimestamp(TopicSource topic, long l) {
            Long time = topic.getTime();
            cuurentTime = Math.max(time, cuurentTime);
            System.out.println("#### currentTime:"+cuurentTime);
            return time;
        }
    }

    public static void main(String[] args) {
        getTopicToHdfsByParquet(env, props);
    }
}
