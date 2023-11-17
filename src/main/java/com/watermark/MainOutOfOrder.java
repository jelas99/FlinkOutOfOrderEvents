package com.watermark;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MainOutOfOrder {

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStateBackend(new HashMapStateBackend());
    env.setParallelism(1);

    DataStream<Event> input = env
        .fromElements(
            new Event("event-1", 1000L),
            new Event("event-1", 2000L),
            new Event("event-1", 1500L), // Out-of-order event
            new Event("event-1", 2500L),
            new Event("event-1", 3000L),
            new Event("event-1", 2900L), // Out-of-order event
            new Event("event-1", 3100L),
            new Event("event-1", 3300L),
            new Event("event-1", 4000L),
            new Event("event-1", 3900L) // Out-of-order event
        )
        .assignTimestampsAndWatermarks(WatermarkStrategy.forGenerator(r -> new WatermarkOnEvent(200L))
            .withTimestampAssigner((event, timestamp) -> event.timestamp()));

    input.keyBy(Event::id).process(new ProcessTimerMapState()).print();

    env.execute("Out-of-Order Events Example");
  }
}
