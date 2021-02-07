package org.apache.flink.training.exercises.hourlytips;

import org.apache.flink.shaded.curator4.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.training.exercises.common.datatypes.TaxiFare;

import java.time.Instant;
import java.util.List;

public class TaxiFareListGenerator implements SourceFunction<TaxiFare> {

	private volatile boolean running = true;

	@Override
	public void run(SourceFunction.SourceContext<TaxiFare> ctx) throws Exception {

		long id = 1;

		List<TaxiFare> list = Lists.newArrayList(
				new TaxiFare(1, 1, 1, Instant.now(), "", 1.1f, 0f, 0f),
				new TaxiFare(2, 2, 2, Instant.now(), "", 1.2f, 0f, 0f),
				new TaxiFare(3, 2, 2, Instant.now(), "", 1.7f, 0f, 0f),
				new TaxiFare(5, 3, 3, Instant.now(), "", 3.4f, 0f, 0f),
				new TaxiFare(4, 1, 1, Instant.now(), "", 1.4f, 0f, 0f),
				new TaxiFare(6, 3, 3, Instant.now(), "", 0.5f, 0f, 0f),

				new TaxiFare(1, 1, 1, Instant.now().plusSeconds(11), "", 2.1f, 0f, 0f),
				new TaxiFare(2, 2, 2, Instant.now().plusSeconds(11), "", 1.2f, 0f, 0f),
				new TaxiFare(3, 2, 2, Instant.now().plusSeconds(11), "", 4.7f, 0f, 0f),
				new TaxiFare(5, 3, 3, Instant.now().plusSeconds(11), "", 3.1f, 0f, 0f),
				new TaxiFare(4, 1, 1, Instant.now().plusSeconds(11), "", 2.4f, 0f, 0f),
				new TaxiFare(6, 3, 3, Instant.now().plusSeconds(11), "", 1.5f, 0f, 0f)
		);

		for (TaxiFare fare : list) {
			ctx.collectWithTimestamp(fare, fare.getEventTime());
			ctx.emitWatermark(new Watermark(fare.getEventTime()));

			Thread.sleep(100);
		}
	}

	@Override
	public void cancel() {
		running = false;
	}
}