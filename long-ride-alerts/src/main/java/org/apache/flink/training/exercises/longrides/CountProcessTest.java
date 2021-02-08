package org.apache.flink.training.exercises.longrides;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction.Context;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction.OnTimerContext;
import org.apache.flink.training.exercises.common.utils.ExerciseBase;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class CountProcessTest {

    public static class CountWithTimeoutFunction
            extends KeyedProcessFunction<String, Tuple2<String, String>, Tuple2<String, Long>> {

        static class CountWithTimestamp {

            public String key;
            public long count;
            public long lastModified;
        }

        private ValueState<CountWithTimestamp> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", CountWithTimestamp.class));
        }

        @Override
        public void processElement(
                Tuple2<String, String> value,
                Context ctx,
                Collector<Tuple2<String, Long>> out) throws Exception {

            // retrieve the current count
            CountWithTimestamp current = state.value();
            if (current == null) {
                current = new CountWithTimestamp();
                current.key = value.f0;
            }

            // update the state's count
            current.count++;


            //ctx.timestamp是事件时间,在ctx.collectWithTimestamp指定的时间
            current.lastModified = ctx.timestamp();
            //System.out.println(value.f0 + "-> " + Instant.now().toEpochMilli() + ":   " + current.lastModified);

            // write the state back
            state.update(current);

            long triggerTime = current.lastModified + 3000; //如果是事件时间，这个时间可能早已过了当前时间
            System.out.println(value.f0 + "==> " + msToStr(Instant.now().toEpochMilli())
                    + "    " + msToStr(ctx.timestamp())
                    + "    " + msToStr(triggerTime));

            // schedule the next timer 60 seconds from the current event time
            ctx.timerService().registerEventTimeTimer(triggerTime);
        }

        private String msToStr(long miliSecond) {
            return LocalDateTime.ofInstant(Instant.ofEpochMilli(miliSecond), ZoneId.of(ZoneId.SHORT_IDS.get("CTT"))).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        }
        private String nowStr() {
            return LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME);
        }

        @Override
        public void onTimer(
                long timestamp,
                OnTimerContext ctx,
                Collector<Tuple2<String, Long>> out) throws Exception {

            // get the state for the key that scheduled the timer
            CountWithTimestamp result = state.value();

            System.out.println(result.key + "--> " + nowStr() + "    " + msToStr(timestamp) +  "    " + result.count);
            // check if this is an outdated timer or the latest timer
            if (timestamp == result.lastModified + 3000) {
                //超过了5秒没有更新
//                System.out.println(result.key + " " + result.count + " " + nowStr() + ":   " + msToStr(timestamp));
                out.collect(Tuple2.of(result.key, result.count));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(ExerciseBase.parallelism);
        DataStream<Tuple2<String, String>> stream = env.addSource(new CountDataGenerator());

        stream.keyBy(value -> value.f0).process(new CountWithTimeoutFunction()).print();

        env.execute("test count");
    }
}