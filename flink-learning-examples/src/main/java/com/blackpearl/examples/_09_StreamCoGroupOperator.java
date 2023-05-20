package com.blackpearl.examples;

import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;


/**
 *
 * 测试数据：
 *   左流：
 *       1,zs
 *       1,zz
 *       2,ls
 *       3,ww
 *       4,zl
 *   右流：
 *       1,18
 *       2,28
 *       3,38
 *       3,48
 *       5,18
 * =============================================
 * 测试结果：
 *   coGroup：(left join 包含 id=4 的数据)
 *       {"age":18,"id":1,"name":"zs"}
 *       {"age":18,"id":1,"name":"zz"}
 *       {"age":0,"id":4,"name":"zl"}
 *       {"age":38,"id":3,"name":"ww"}
 *       {"age":48,"id":3,"name":"ww"}
 *       {"age":28,"id":2,"name":"ls"}
 *   join：(inner join 不包含 id=4 的数据)
 *       {"age":18,"id":1,"name":"zs"}
 *       {"age":18,"id":1,"name":"zz"}
 *       {"age":38,"id":3,"name":"ww"}
 *       {"age":48,"id":3,"name":"ww"}
 *       {"age":28,"id":2,"name":"ls"}
 */


public class _09_StreamCoGroupOperator {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // id, name
        DataStreamSource<String> socket1 = env.socketTextStream("node01", 9998);
        SingleOutputStreamOperator<Tuple2<Integer, String>> stream1 = socket1.map((MapFunction<String, Tuple2<Integer, String>>) s -> {
            String[] fields = s.split(",");
            return Tuple2.of(Integer.parseInt(fields[0]), fields[1]);
        }).returns(new TypeHint<Tuple2<Integer, String>>() {
        });

        // id, age
        DataStreamSource<String> socket2 = env.socketTextStream("node01", 9999);
        SingleOutputStreamOperator<Tuple2<Integer, Integer>> stream2 = socket2.map(s -> {
            String[] fields = s.split(",");
            return Tuple2.of(Integer.parseInt(fields[0]), Integer.parseInt(fields[1]));
        }).returns(new TypeHint<Tuple2<Integer, Integer>>() {
        });


        /**
         *  coGroup：
         *  该算子涉及窗口计算，所以只有在同一窗口中的两个流的数据才能join
         *  在测试中，如果某个流输入的数据较慢，可能导致两个流的测试数据不在同一窗口中，运行时显示不出join效果
         */
        DataStream<UserInfo> applied = stream1.coGroup(stream2)
                .where(tp -> tp.f0)
                .equalTo(tp -> tp.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
                .apply(new CoGroupFunction<Tuple2<Integer, String>, Tuple2<Integer, Integer>, UserInfo>() {
                    @Override
                    public void coGroup(Iterable<Tuple2<Integer, String>> left, Iterable<Tuple2<Integer, Integer>> right, Collector<UserInfo> collector) throws Exception {

                        // 示例：做个left join
                        for (Tuple2<Integer, String> leftItem : left) {
                            boolean flag = false;
                            for (Tuple2<Integer, Integer> rightItem : right) {
                                UserInfo userInfo = new UserInfo(leftItem.f0, leftItem.f1, rightItem.f1);
                                collector.collect(userInfo);
                                flag = true;
//                                System.out.println("left:: "+leftItem);
//                                System.out.println("right:: "+rightItem);
                            }

                            if (!flag) {
                                UserInfo userInfo = new UserInfo(leftItem.f0, leftItem.f1, 0);
                                collector.collect(userInfo);
                            }
                        }


                    }
                });
        applied.map(JSON::toJSONString).print();


        /**
         * join：
         * 改算子处理数据方式为 inner join
         */
        DataStream<UserInfo> joined = stream1.join(stream2)
                .where(tp -> tp.f0)
                .equalTo(tp -> tp.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(20)))
                .apply(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, Integer>, UserInfo>() {
                    @Override
                    public UserInfo join(Tuple2<Integer, String> tp1, Tuple2<Integer, Integer> tp2) throws Exception {
                        return new UserInfo(tp1.f0, tp1.f1, tp2.f1);
                    }
                });
//        joined.map(JSON::toJSONString).print();



        env.execute("co-group-operator");

    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    private static class UserInfo {
        private int id;
        private String name;
        private int age;
    }


}
