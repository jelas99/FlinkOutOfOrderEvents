package com.watermark.alignment;

import static org.apache.commons.collections.CollectionUtils.isEmpty;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * We are using MapState because is more performance with RocksDb.
 * With RocksDb the MapState serialization occurs per-entry.
 *
 *
 */
public class ProcessTimerMapState extends KeyedProcessFunction<String, Event, Event> {

  private transient MapState<Long, List<Event>> queueState = null;

  @Override
  public void open(Configuration config) {
    TypeInformation<Long> key = TypeInformation.of(new TypeHint<Long>() {});
    TypeInformation<List<Event>> value = TypeInformation.of(new TypeHint<List<Event>>() {});

    queueState = getRuntimeContext().getMapState(new MapStateDescriptor<>("messages-timestamp", key, value));
  }

  @Override
  public void processElement(Event value, KeyedProcessFunction<String, Event, Event>.Context ctx,
      Collector<Event> out) throws Exception {

      TimerService timerService = ctx.timerService();

      if (ctx.timestamp() > timerService.currentWatermark()) {
        List<Event> listEvents = queueState.get(value.timestamp());
        if(isEmpty(listEvents)) {
          listEvents = new ArrayList<>();
        }

        listEvents.add(value);
        queueState.put(value.timestamp(), listEvents);
        timerService.registerEventTimeTimer(value.timestamp());
    }
  }

  @Override
  public void onTimer(long timestamp, OnTimerContext ctx, Collector<Event> out)
      throws Exception {
    
    Long watermark = ctx.timerService().currentWatermark();
    List<Long> sortedTimestamps = StreamSupport.stream(queueState.keys().spliterator(), false)
        .sorted()
        .collect(Collectors.toList());

    for (var timeState : sortedTimestamps) {
      if(timeState <= watermark) {
        queueState.get(timeState).forEach(out::collect);
        queueState.remove(timeState);
      } else {
        break;
      }
    }
  }
}

