/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.training.exercises.hourlytips;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.training.exercises.common.datatypes.TaxiFare;
import org.apache.flink.training.exercises.common.sources.TaxiFareGenerator;
import org.apache.flink.training.exercises.common.utils.ExerciseBase;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.functions.AggregateFunction;

import javax.swing.*;
import java.lang.reflect.Type;
import java.time.Duration;
import java.time.Instant;

/**
 * The "Hourly Tips" exercise of the Flink training in the docs.
 *
 * <p>The task of the exercise is to first calculate the total tips collected by each driver, hour by hour, and
 * then from that stream, find the highest tip total in each hour.
 *
 */
public class HourlyTipsExercise extends ExerciseBase {

	private static class IncSumTip implements AggregateFunction<TaxiFare, Tuple2<Long, Float>, Tuple2<Long, Float>> {
		@Override
		public Tuple2<Long, Float> createAccumulator() {
			return Tuple2.of(0L, 0F);
		}
		@Override
		public Tuple2<Long, Float> add(TaxiFare value, Tuple2<Long, Float> accumulator) {
			return Tuple2.of(value.driverId, accumulator.f1 + value.tip);
		}
		@Override
		public Tuple2<Long, Float> getResult(Tuple2<Long, Float> accumulator) {
			return accumulator;
		}
		@Override
		public Tuple2<Long, Float> merge(Tuple2<Long, Float> a, Tuple2<Long, Float> b) {
			return Tuple2.of(a.f0, a.f1 + b.f1);
		}
	}

	private static class MaxFunc implements AggregateFunction<Tuple3<Long, Long, Float>,Tuple3<Long, Long, Float>,Tuple3<Long, Long, Float>> {
		@Override
		public Tuple3<Long, Long, Float> createAccumulator() {
			return Tuple3.of(0L, 0L, 0F);
		}
		@Override
		public Tuple3<Long, Long, Float> add(Tuple3<Long, Long, Float> value, Tuple3<Long, Long, Float> accumulator) {
			return value.f2 > accumulator.f2 ? value : accumulator;
		}
		@Override
		public Tuple3<Long, Long, Float> getResult(Tuple3<Long, Long, Float> accumulator) {
			return accumulator;
		}
		@Override
		public Tuple3<Long, Long, Float> merge(Tuple3<Long, Long, Float> a, Tuple3<Long, Long, Float> b) {
			return a.f2 > b.f2 ? a : b;
		}
	}
//	private static class SumAllTip extends ProcessWindowFunction<TaxiFare, Tuple3<Long, Long, Float>, Long, TimeWindow> {
//		@Override
//		public void process(Long key, Context context, Iterable<TaxiFare> elements, Collector<Tuple3<Long, Long, Float>> out) throws Exception {
//			float sum = 0f;
//			for (TaxiFare fare : elements) {
//				sum += fare.tip;
//			}
//			out.collect(Tuple3.of(context.window().getStart(), key, sum));
//		}
//	}

	private static class CollectIncTip extends ProcessWindowFunction<Tuple2<Long, Float>, Tuple3<Long, Long, Float>, Long, TimeWindow> {
		@Override
		public void process(Long aLong, Context context, Iterable<Tuple2<Long, Float>> elements, Collector<Tuple3<Long, Long, Float>> out) throws Exception {
			Tuple2<Long, Float> sum = elements.iterator().next();
			out.collect(Tuple3.of(context.window().getStart(), aLong, sum.f1));
		}
	}



	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(ExerciseBase.parallelism);
//		DataStream<TaxiFare> fares = env.addSource(fareSourceOrTest(new TaxiFareGenerator()));

		DataStream<TaxiFare> fares = env.addSource(new TaxiFareListGenerator());

		fares.keyBy((TaxiFare fare) -> fare.driverId)
			.window(TumblingEventTimeWindows.of(Time.seconds(10)))
			.aggregate(new IncSumTip(), new CollectIncTip())
			.keyBy(t -> t.f0)
//			.reduce((t1,t2)->t1.f2 > t2.f2 ? t1 : t2)
			.window(TumblingEventTimeWindows.of(Time.seconds(10)))
			.max(2)
//			.aggregate(new MaxFunc())
			.print();
		//new SumAggregator("totalFare", TypeInformation.of(new TypeHint<Tuple2<Long, Float>>(){}), null)

//		printOrTest(hourlyMax);
		env.execute("Hourly Tips (java)");
	}

}
