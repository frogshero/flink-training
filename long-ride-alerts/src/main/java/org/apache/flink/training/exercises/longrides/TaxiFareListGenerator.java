package org.apache.flink.training.exercises.longrides;

import org.apache.flink.shaded.curator4.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;

import java.time.Instant;
import java.util.List;

public class TaxiRideListGenerator implements SourceFunction<TaxiRide> {

	private volatile boolean running = true;

	@Override
	public void run(SourceContext<TaxiRide> ctx) throws Exception {

		long id = 1;

		List<TaxiRide> list = Lists.newArrayList(
				new TaxiRide(1, true, Instant.now(), Instant.now(), 0f, 1.1f, 0f, 0f, (short) 0, 1, 1),
				new TaxiRide(1, false, Instant.now(), Instant.now(), 0f, 1.2f, 0f, 0f, (short) 0, 1, 1),
				new TaxiRide(2, true, Instant.now(), Instant.now(), 0f, 1.7f, 0f, 0f, (short) 0, 2, 2),
				new TaxiRide(2, false, Instant.now(), Instant.now(), 0f, 3.4f, 0f, 0f, (short) 0, 2, 2),
				new TaxiRide(3, true, Instant.now(), Instant.now(), 0f, 1.4f, 0f, 0f, (short) 0, 3, 3),
				new TaxiRide(3, false, Instant.now(), Instant.now(), 0f, 0.5f, 0f, 0f, (short) 0, 3,3),
				
		);

		for (TaxiRide ride : list) {
			ctx.collectWithTimestamp(ride, ride.getEventTime());
			ctx.emitWatermark(new Watermark(ride.getEventTime()));
			Thread.sleep(100);
		}
	}

	@Override
	public void cancel() {
		running = false;
	}
}