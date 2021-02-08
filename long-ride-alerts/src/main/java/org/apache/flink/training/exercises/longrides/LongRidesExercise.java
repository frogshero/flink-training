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

package org.apache.flink.training.exercises.longrides;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;
import org.apache.flink.training.exercises.common.utils.ExerciseBase;
import org.apache.flink.util.Collector;

/**
 * The "Long Ride Alerts" exercise of the Flink training in the docs.
 *
 * <p>The goal for this exercise is to emit START events for taxi rides that have not been matched
 * by an END event during the first 2 hours of the ride.
 *
 */
public class LongRidesExercise extends ExerciseBase {

	/**
	 * Main method.
	 *
	 * @throws Exception which occurs during job execution.
	 */
	public static void main(String[] args) throws Exception {

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(ExerciseBase.parallelism);

		// start the data generator
		DataStream<TaxiRide> rides = env.addSource(new TaxiRideListGenerator());

		DataStream<TaxiRide> longRides = rides
				.keyBy((TaxiRide ride) -> ride.rideId)
				.process(new MatchFunction());

		printOrTest(longRides);

		env.execute("Long Taxi Rides");
	}

	public static class MatchFunction extends KeyedProcessFunction<Long, TaxiRide, TaxiRide> {
		ValueState<Long> lastStartTime;
		ValueState<TaxiRide> lastStartRide;

		@Override
		public void open(Configuration config) throws Exception {
			ValueStateDescriptor desc = new ValueStateDescriptor("StateTimer", TypeInformation.of(new TypeHint<Long>(){}));
			lastStartTime = getRuntimeContext().getState(desc);

			ValueStateDescriptor desc2 = new ValueStateDescriptor("StateRide", TypeInformation.of(new TypeHint<TaxiRide>(){}));
			lastStartRide = getRuntimeContext().getState(desc2);
		}

		@Override
		public void processElement(TaxiRide ride, Context context, Collector<TaxiRide> out) throws Exception {
			try {
				TimerService timerService = context.timerService();
				if (ride.isStart) {
					//调用ctx.emitWatermark设定watermark，如果watermark没有超过timeout时间，不会触发
					lastStartTime.update(context.timestamp() + 1000);
					System.out.println(context.timestamp());
					timerService.registerEventTimeTimer(lastStartTime.value());

					lastStartRide.update(ride);
				} else {
					System.out.println(lastStartTime.value());
					timerService.deleteEventTimeTimer(lastStartTime.value());
					lastStartRide.clear();
					lastStartTime.clear();
//					lastStartTime.update(null);
//					lastStartRide.update(null);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		@Override
		public void onTimer(long timestamp, OnTimerContext context, Collector<TaxiRide> out) throws Exception {
			out.collect(lastStartRide.value());
		}
	}
}
