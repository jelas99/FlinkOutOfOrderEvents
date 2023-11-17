package com.watermark.alignment;

import java.io.Serializable;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

public class WatermarkOnEvent implements WatermarkGenerator<Event>, Serializable {
  private final long MaxOutOfOrdeness;

  public WatermarkOnEvent(long MaxOutOfOrdeness) {
    this.MaxOutOfOrdeness = MaxOutOfOrdeness;
  }

  @Override
  public void onEvent(Event event, long eventTimestamp, WatermarkOutput output) {
      long watermark = eventTimestamp - MaxOutOfOrdeness;
      output.emitWatermark(new Watermark(watermark));
  }

  @Override
  public void onPeriodicEmit(WatermarkOutput output) {
  }
}
