package com.blackpearl.gmall.app.dwm;

import com.alibaba.fastjson.JSON;
import com.blackpearl.gmall.bean.OrderWide;
import com.blackpearl.gmall.bean.PaymentInfo;
import com.blackpearl.gmall.bean.PaymentWide;
import com.blackpearl.gmall.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class PaymentWideApp {

    public static final String GROUP_PAYMENT_WIDE = "payment_wide_app";
    public static final String TOPIC_DWD_ORDER_WIDE = "dwm_order_wide";
    public static final String TOPIC_DWD_PAYMENT_INFO = "dwd_payment_info";
    public static final String TOPIC_DWM_PAYMENT_WIDE = "dwm_payment_wide";

    public static void main(String[] args) throws Exception {

        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 读取kafka中dwd_payment_info和dwd_order_wide主题的数据，转换数据格式String => JavaBean，提取时间戳生成WaterMark
        SingleOutputStreamOperator<OrderWide> orderWideStream = env.fromSource(KafkaUtil.getKafkaSource(TOPIC_DWD_ORDER_WIDE, GROUP_PAYMENT_WIDE), WatermarkStrategy.noWatermarks(), "order-wide-stream")
                .map(line -> JSON.parseObject(line, OrderWide.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderWide>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {
                    @Override
                    public long extractTimestamp(OrderWide orderWide, long l) {
                        String createTime = orderWide.getCreate_time(); // TODO 还不清楚是什么类型，测试时注意改
                        return Long.parseLong(createTime);
                    }
                }));

        SingleOutputStreamOperator<PaymentInfo> paymentInfoStream = env.fromSource(KafkaUtil.getKafkaSource(TOPIC_DWD_PAYMENT_INFO, GROUP_PAYMENT_WIDE), WatermarkStrategy.noWatermarks(), "payment-info-stream")
                .map(line -> JSON.parseObject(line, PaymentInfo.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<PaymentInfo>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
                    @Override
                    public long extractTimestamp(PaymentInfo paymentInfo, long l) {
                        String createTime = paymentInfo.getCreate_time(); // TODO 还不清楚是什么类型，测试时注意改
                        return Long.parseLong(createTime);
                    }
                }));

        // 3. 双流Join：intervalJoin ->  注意时间范围 -> 下单后需要在15分钟内支付
        SingleOutputStreamOperator<PaymentWide> paymentWideStream = paymentInfoStream.keyBy(PaymentInfo::getOrder_id)
                .intervalJoin(orderWideStream.keyBy(OrderWide::getOrder_id))
                .between(Time.minutes(-15), Time.seconds(30))
                .process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                    @Override
                    public void processElement(PaymentInfo paymentInfo, OrderWide orderWide, ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>.Context context, Collector<PaymentWide> collector) throws Exception {

                        collector.collect(new PaymentWide(paymentInfo, orderWide));
                    }
                });

        // 4. 将结果数据写入到kafka
        paymentWideStream.print();
        paymentWideStream
                .map(JSON::toJSONString)
                .sinkTo(KafkaUtil.getKafkaSink(TOPIC_DWM_PAYMENT_WIDE));

        // 5. 任务执行
        env.execute("payment-wide-app");

    }

}
