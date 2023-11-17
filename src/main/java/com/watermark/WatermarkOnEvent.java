package com.watermark;

import java.io.Serializable;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

public class WatermarkOnEvent implements WatermarkGenerator<Event>, Serializable {
  private final long maxOutOfOrdeness;

  public WatermarkOnEvent(long maxOutOfOrdeness) {
    this.maxOutOfOrdeness = maxOutOfOrdeness;
  }

  @Override
  public void onEvent(Event event, long eventTimestamp, WatermarkOutput output) {
      long watermark = eventTimestamp - maxOutOfOrdeness;
      output.emitWatermark(new Watermark(watermark));
  }

  @Override
  public void onPeriodicEmit(WatermarkOutput output) {
    // We want to emit watermark by event
  }
}
