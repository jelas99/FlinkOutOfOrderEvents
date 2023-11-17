package com.watermark.alignment;

import java.util.PriorityQueue;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class ProcessTimer extends KeyedProcessFunction<String, Event, Event> {

  private transient ValueState<PriorityQueue<Event>> queueState = null;

  @Override
  public void open(Configuration config) {
    ValueStateDescriptor<PriorityQueue<Event>> descriptor = new ValueStateDescriptor<>(
        "sorted-events",
        TypeInformation.of(new TypeHint<PriorityQueue<Event>>() {
        }));
    queueState = getRuntimeContext().getState(descriptor);
  }

  @Override
  public void processElement(Event value, KeyedProcessFunction<String, Event, Event>.Context ctx,
      Collector<Event> out) throws Exception {

    TimerService timerService = ctx.timerService();

    if (ctx.timestamp() > timerService.currentWatermark()) {
      PriorityQueue<Event> queue = queueState.value();
      if (queue == null) {
        queue = new PriorityQueue<>(10);
      }
      queue.add(value);
      queueState.update(queue);
      timerService.registerEventTimeTimer(value.timestamp());
    }

  }

  @Override
  public void onTimer(long timestamp, OnTimerContext ctx, Collector<Event> out)
      throws Exception {



    PriorityQueue<Event> queue = queueState.value();
    Long watermark = ctx.timerService().currentWatermark();
    Event head = queue.peek();

    while (head != null && head.timestamp() <= watermark) {
      ///System.out.println("Message ----> " + head.timestamp() + "----> " +watermark);

      out.collect(head);
      queue.remove(head);
      head = queue.peek();
    }
  }

}

