package org.apache.flink.training.exercises.longrides;

import org.apache.flink.shaded.curator4.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class TaxiRideListGenerator implements SourceFunction<TaxiRide> {

	private volatile boolean running = true;

	@Override
	public void run(SourceContext<TaxiRide> ctx) throws Exception {

		long baseTime = Instant.now().minusSeconds(3600 * 24).toEpochMilli();
		long id = 1;

		List<TaxiRide> list = Lists.newArrayList(
				new TaxiRide(1, true, Instant.ofEpochMilli(baseTime), null, 1)

				//下面这条的startTime会导致上面这一条timeout
				,new TaxiRide(2, true, Instant.ofEpochMilli(baseTime + 2000), null, 2)
				,new TaxiRide(1, false, null, Instant.ofEpochMilli(baseTime + 1000), 1)
				,new TaxiRide(2, false, null, Instant.ofEpochMilli(baseTime + 3000), 2)
				
		);

		for (TaxiRide ride : list) {
			ctx.collectWithTimestamp(ride, ride.getEventTime());
			ctx.emitWatermark(new Watermark(ride.getEventTime()));
			Thread.sleep(1000);
		}
	}

	@Override
	public void cancel() {
		running = false;
	}
}