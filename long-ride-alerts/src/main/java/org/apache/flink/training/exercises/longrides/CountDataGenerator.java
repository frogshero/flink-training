package org.apache.flink.training.exercises.longrides;


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;

import java.time.Instant;
import java.time.temporal.TemporalUnit;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class CountDataGenerator implements SourceFunction<Tuple2<String, String>> {
    private volatile boolean running = true;

    private long baseTime = Instant.now().minusSeconds(3600 * 24).toEpochMilli();

    @Override
    public void run(SourceContext<Tuple2<String, String>> ctx) throws Exception {

        emitRecord(ctx, Tuple2.of("1", "11"), 0);
        emitRecord(ctx, Tuple2.of("1", "111"), 1);

//        Thread.sleep(5000);
        emitRecord(ctx, Tuple2.of("2", "22"), 2);
        emitRecord(ctx, Tuple2.of("2", "222"), 3);
        emitRecord(ctx, Tuple2.of("1", "11111"), 4);
    }

    private void emitRecord(SourceContext<Tuple2<String, String>> ctx, Tuple2<String, String> record, int i) throws InterruptedException {
        ctx.collectWithTimestamp(record, baseTime + i * 2000);

        //表示小于这个参数的时间戳的事件不会出现，如果出现就会被认为是late
        ctx.emitWatermark(new Watermark(baseTime + i * 2000));
        Thread.sleep(1000);
    }

    @Override
    public void cancel() {
        running = false;
    }
}
