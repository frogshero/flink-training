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

package org.apache.flink.training.examples.ridecount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.base.Strings;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;
import org.apache.flink.training.exercises.common.sources.TaxiRideGenerator;
import org.apache.flink.util.Collector;

/**
 * Example that counts the rides for each driver.
 *
 * <p>Note that this is implicitly keeping state for each driver.
 * This sort of simple, non-windowed aggregation on an unbounded set of keys will use an unbounded amount of state.
 * When this is an issue, look at the SQL/Table API, or ProcessFunction, or state TTL, all of which provide
 * mechanisms for expiring state for stale keys.
 */
public class RideCountExample {

	public static class CntFunction extends RichFlatMapFunction<TaxiRide, Tuple2<Long, Long>> {
		private transient ValueState<Tuple2<Long, Long>> state;
		@Override
		public void open(Configuration parameters) throws Exception {
			ValueStateDescriptor<Tuple2<Long, Long>> desc = new ValueStateDescriptor<>(
					"cnt",
					TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {}));
			StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.seconds(3))
//					.cleanupFullSnapshot()
//					.cleanupIncrementally()
					.setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
					.setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
					.build();
			desc.enableTimeToLive(ttlConfig);
			state = getRuntimeContext().getState(desc);

//Caused by: java.lang.NullPointerException: No key set. This method should not be called outside of a keyed context.
//			if (state.value() == null) {
//				state.update(Tuple2.of(0L, 0L));
//			}
		}
		@Override
		public void flatMap(TaxiRide value, Collector<Tuple2<Long, Long>> out) throws Exception {
			//必须在KeyStream之后
			Tuple2<Long, Long> val = state.value();
			if (val == null) {
				val = Tuple2.of(value.driverId, 0L);
			}
			val.f1++;
			state.update(val);
			out.collect(val);
			System.out.println(Thread.currentThread().getName() + ": " + val + " " + this.hashCode() + " " + state.hashCode());
		}
	}

	public static void main(String[] args) throws Exception {
		myTest();
	}

	private static void myTest() throws Exception {
		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// start the data generator
		TaxiRideGenerator trg = new TaxiRideGenerator();
		//trg和实际运行的对象不一样
		System.out.println(Strings.repeat("-", 50) + trg.hashCode());
		DataStream<TaxiRide> rides = env.addSource(trg);

		//Group后再Map
		rides.keyBy(t -> t.driverId).flatMap(new CntFunction()).print();
		//2> (2013000145,13) : 线程ID> (driverId,count)

		env.execute("Ride Count");
	}

	private static void origintalTest() throws Exception {
		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// start the data generator
		TaxiRideGenerator trg = new TaxiRideGenerator();
		//trg和实际运行得对象不一样
		System.out.println(Strings.repeat("-", 50) + trg.hashCode());
		DataStream<TaxiRide> rides = env.addSource(trg);

		// map each ride to a tuple of (driverId, 1)
		DataStream<Tuple2<Long, Long>> tuples = rides.map(new MapFunction<TaxiRide, Tuple2<Long, Long>>() {
			@Override
			public Tuple2<Long, Long> map(TaxiRide ride) {
				return Tuple2.of(ride.driverId, 1L);
			}
		});

		// partition the stream by the driverId
		KeyedStream<Tuple2<Long, Long>, Long> keyedByDriverId = tuples.keyBy(t -> t.f0);

		// count the rides for each driver
		DataStream<Tuple2<Long, Long>> rideCounts = keyedByDriverId.sum(1);

		// we could, in fact, print out any or all of these streams
		rideCounts.print();

		// run the cleansing pipeline
		env.execute("Ride Count");
	}


}
