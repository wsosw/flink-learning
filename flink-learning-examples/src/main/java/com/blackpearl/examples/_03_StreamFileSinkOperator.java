package com.blackpearl.examples;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;

public class _03_StreamFileSinkOperator {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///f:/flink/ckpt");

        DataStreamSource<EventLog> source = env.addSource(new MySourceFunction());

        // 打印到控制台
        source.print();

        // 输出到普通文本文件（过时）
        //source.map(JSON::toJSONString).writeAsText("f:\\flink\\aaa.txt", FileSystem.WriteMode.OVERWRITE);

        // 输出到csv文件（过时）
        // 注意：写出数据到csv文件时，flink默认攒够4096byte后才会写出到文件，如果数据过少（或数据产生速度过慢）可能在短时间看不到数据
//        source.map(event -> Tuple5.of(event.getEventId(), event.getGuid(), event.getEventInfo(), event.getSessionId(), event.getTimeStamp()))
//                .returns(new TypeHint<Tuple5<String, Long, Map<String, String>, String, Long>>() {})
//                .writeAsCsv("f:\\flink\\aaa.csv", FileSystem.WriteMode.OVERWRITE);


        // 按行格式输出到parquet文件
        FileSink<String> rowSink = FileSink.forRowFormat(new Path("f:/flink/row"), new SimpleStringEncoder<String>("utf-8"))
                // 文件滚动策略：文件大小到达5MB或间隔1min滚动一次（任一条件满足，即发生滚动）
                .withRollingPolicy(DefaultRollingPolicy.builder().withMaxPartSize(5 * 1024 * 1024).withRolloverInterval(10 * 1000).build())
                // 文件分桶策略（即分文件夹策略）：DateTimeBucketAssigner默认按小时分：yyyy-MM-dd--HH
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd--HH--mm"))
                // 桶（文件夹）检查间隔：主要用来提前判断创建新的文件夹（X） => 视频中的解释应该是错误的
                // 经测试，该参数对滚动策略也有影响，比如10秒滚动一次，但当检查间隔设置为20000（20秒）时，文件在经过10秒后并没有发生滚动，而在经过20秒后才发生滚动
                // 因此，该参数不是用来提前检查和判断生成文件夹的，应该是对桶中的文件进行检查：以上withRollingPolicy只是定义了滚动策略，但需要该参数进行检查后才能触发
                // 该参数默认为60000L（一分钟），当文件滚动频繁时，该参数应设置为一个很小的值，否则无法及时触发滚动策略，生成新文件
                .withBucketCheckInterval(5)
                // 输出文件配置：主要是文件名的前缀和后缀
                .withOutputFileConfig(OutputFileConfig.builder().withPartPrefix("xx").withPartSuffix(".txt").build())
                .build();
        // source.map(JSON::toJSONString).sinkTo(rowSink);


        // 按列格式输出到parquet文件（按列格式输出时，必须开启checkpoint机制）
        ParquetWriterFactory<EventLog> factory = ParquetAvroWriters.forReflectRecord(EventLog.class);
        FileSink<EventLog> bulkSink = FileSink.forBulkFormat(new Path("f:/flink/bulk"), factory)
                // 列格式下只能使用OnCheckpointRollingPolicy滚动策略：即在checkpoint时，将本批数据写入文件
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .withBucketAssigner(new DateTimeBucketAssigner<>())
                .withBucketCheckInterval(5)
                .withOutputFileConfig(OutputFileConfig.builder().withPartPrefix("xx").withPartSuffix(".parquet").build())
                .build();
        source.sinkTo(bulkSink);


        env.execute("stream-file-sink");
    }


}
