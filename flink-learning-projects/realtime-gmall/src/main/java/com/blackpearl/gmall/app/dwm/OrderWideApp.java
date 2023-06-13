package com.blackpearl.gmall.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.blackpearl.gmall.bean.OrderDetail;
import com.blackpearl.gmall.bean.OrderInfo;
import com.blackpearl.gmall.bean.OrderWide;
import com.blackpearl.gmall.function.DimAsyncFunction;
import com.blackpearl.gmall.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class OrderWideApp {

    public static final String GROUP_ORDER_WIDE_APP = "order_wide_app";
    public static final String TOPIC_DWD_ORDER_INFO = "dwd_order_info";
    public static final String TOPIC_DWD_ORDER_DETAIL = "dwd_order_detail";
    public static final String TOPIC_DWM_ORDER_WIDE = "dwm_order_wide";

    public static void main(String[] args) throws Exception {

        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 读取kafka中dwd_order_info和dwd_order_detail主题中的数据，并转换数据格式（String -> JavaBean），提取时间戳生成WaterMark
        DataStreamSource<String> orderInfoStringStream = env.fromSource(KafkaUtil.getKafkaSource(TOPIC_DWD_ORDER_INFO, GROUP_ORDER_WIDE_APP), WatermarkStrategy.noWatermarks(), "order-info-stream");
        SingleOutputStreamOperator<OrderInfo> orderInfoStream = orderInfoStringStream.map(line -> {
            OrderInfo orderInfo = JSON.parseObject(line, OrderInfo.class);
            String createTime = orderInfo.getCreate_time(); // "2023-05-13 14:52:00"
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            long timeStamp = simpleDateFormat.parse(createTime).getTime();

            String[] splits = createTime.split(" ");
            orderInfo.setCreate_date(splits[0]);
            orderInfo.setCreate_hour(splits[1].split(":")[0]);
            orderInfo.setCreate_ts(timeStamp);

            return orderInfo;
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderInfo>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
            @Override
            public long extractTimestamp(OrderInfo orderInfo, long l) {
                return orderInfo.getCreate_ts();
            }
        }));


        DataStreamSource<String> orderDetailStringStream = env.fromSource(KafkaUtil.getKafkaSource(TOPIC_DWD_ORDER_DETAIL, GROUP_ORDER_WIDE_APP), WatermarkStrategy.noWatermarks(), "order-detail-stream");
        SingleOutputStreamOperator<OrderDetail> orderDetailStream = orderDetailStringStream.map(line -> {

            OrderDetail orderDetail = JSON.parseObject(line, OrderDetail.class);
            String createTime = orderDetail.getCreate_time(); // "2023-05-13 14:52:00"
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            long timeStamp = simpleDateFormat.parse(createTime).getTime();

            orderDetail.setCreate_ts(timeStamp);

            return orderDetail;
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderDetail>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
            @Override
            public long extractTimestamp(OrderDetail orderDetail, long l) {
                return orderDetail.getCreate_ts();
            }
        }));

        // 3. 双流Join：OrderInfo.join(OrderDetail)
        SingleOutputStreamOperator<OrderWide> orderWideWithNoDimStream = orderInfoStream
                .keyBy(OrderInfo::getId)
                .intervalJoin(orderDetailStream.keyBy(OrderDetail::getOrder_id))
                .between(Time.seconds(-5), Time.seconds(5)) // join 左流当前数据的上下5秒之内的数据
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>.Context context, Collector<OrderWide> collector) throws Exception {
                        collector.collect(new OrderWide(orderInfo, orderDetail));
                    }
                });

        // 4. 关联Hbase（Phoenix）中的其他维度信息：用户，地区，SKU，SPU，TM，Category
        // 关联用户维度
        SingleOutputStreamOperator<OrderWide> orderWideWithUserStream = AsyncDataStream.unorderedWait(
                orderWideWithNoDimStream, new DimAsyncFunction<OrderWide>("DIM_USER_INFO") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getUser_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {
                        orderWide.setUser_gender(dimInfo.getString("GENDER"));

                        // 以下是一个简单的日期转换， 并不标准
                        String birthday = dimInfo.getString("BIRTHDAY"); // "2023-01-01"
                        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
                        long timeStamp = simpleDateFormat.parse(birthday).getTime();
                        long currenTs = System.currentTimeMillis();
                        long age = (currenTs - timeStamp) / (1000 * 60 * 60 * 24 * 365L);
                        orderWide.setUser_age((int) age);
                    }
                }, 60, TimeUnit.SECONDS);

        // 关联地区维度
        SingleOutputStreamOperator<OrderWide> orderWideWithProvinceStream = AsyncDataStream.unorderedWait(
                orderWideWithUserStream, new DimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getProvince_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) {
                        orderWide.setProvince_name(dimInfo.getString("NAME"));
                        orderWide.setProvince_area_code(dimInfo.getString("AREA_CODE"));
                        orderWide.setProvince_iso_code(dimInfo.getString("ISO_CODE"));
                        orderWide.setProvince_3166_2_code(dimInfo.getString("ISO_3166_2"));
                    }
                }, 60, TimeUnit.SECONDS);

        // 关联SKU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSkuStream = AsyncDataStream.unorderedWait(
                orderWideWithProvinceStream, new DimAsyncFunction<OrderWide>("DIM_SKU_INFO") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getSku_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) {
                        orderWide.setSku_name(jsonObject.getString("SKU_NAME"));
                        orderWide.setCategory3_id(jsonObject.getLong("CATEGORY3_ID"));
                        orderWide.setSpu_id(jsonObject.getLong("SPU_ID"));
                        orderWide.setTm_id(jsonObject.getLong("TM_ID"));
                    }

                }, 60, TimeUnit.SECONDS);

        // 关联SPU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSpuStream = AsyncDataStream.unorderedWait(
                orderWideWithSkuStream, new DimAsyncFunction<OrderWide>("DIM_SPU_INFO") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getSpu_id());
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) {
                        orderWide.setSpu_name(jsonObject.getString("SPU_NAME"));
                    }
                }, 60, TimeUnit.SECONDS);

        // 关联TM维度
        SingleOutputStreamOperator<OrderWide> orderWideWithTmStream = AsyncDataStream.unorderedWait(
                orderWideWithSpuStream, new DimAsyncFunction<OrderWide>("DIM_BASE_TRADEMARK") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getTm_id());
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) {
                        orderWide.setTm_name(jsonObject.getString("TM_NAME"));
                    }
                }, 60, TimeUnit.SECONDS);

        // 关联Category维度
        SingleOutputStreamOperator<OrderWide> orderWideDataStream = AsyncDataStream.unorderedWait(
                orderWideWithTmStream, new DimAsyncFunction<OrderWide>("DIM_BASE_CATEGORY3") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getCategory3_id());
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) {
                        orderWide.setCategory3_name(jsonObject.getString("NAME"));
                    }
                }, 60, TimeUnit.SECONDS);

        // 5. 将数据写入到kafka
        orderWideDataStream.print("[orderwide]");
        orderInfoStream
                .map(JSON::toJSONString)
                .sinkTo(KafkaUtil.getKafkaSink(TOPIC_DWM_ORDER_WIDE));

        // 6. 任务执行
        env.execute("order-wide-app");

    }
}
