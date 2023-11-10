package com.watermark.alignment;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.Serializable;
import java.time.Duration;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {


  public static void main(String[] args) throws Exception {
    Main flink  = new Main();
    flink.start();
  }

  public void start() throws Exception {
    Path pathSourceOne = new Path("src/main/resources/watermark/test-watermark.json");
    Path pathSourceTwo = new Path("src/main/resources/watermark/test-watermark-ahead.json");
    LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(2);
    FileSource<String> sourceOne = getSource(pathSourceOne);
    FileSource<String> sourceTwo = getSource(pathSourceTwo);

    WatermarkStrategy<String> strategy = WatermarkStrategy.forGenerator(
            ctx -> new WatermarkOnEvent()).withIdleness(Duration.ofSeconds(5))
        .withTimestampAssigner(new TimeAssigner())
        .withWatermarkAlignment("alignment", Duration.ofSeconds(1), Duration.ofMillis(200));

    DataStreamSource<String> one = env.fromSource(sourceOne, strategy, "uidOne");

    DataStreamSource<String> two = env.fromSource(sourceTwo, strategy, "uidTwo");

    one.union(two).print();

    env.execute();

  }

  private FileSource<String> getSource(Path path) {
    return FileSource.forRecordStreamFormat(new TextLineInputFormat(), path).build();
  }

  static class WatermarkOnEvent implements WatermarkGenerator<String>, Serializable {

    private static final Logger LOGGER
        = LoggerFactory.getLogger(WatermarkOnEvent.class);
    @Override
    public void onEvent(String event, long eventTimestamp, WatermarkOutput output) {
      Watermark watermark = new Watermark(eventTimestamp);
      LOGGER.info("Watermark emitted: {} ", watermark);

      output.emitWatermark(watermark);
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
    }
  }

  static class TimeAssigner implements SerializableTimestampAssigner<String> {

    final ObjectMapper mapper = new ObjectMapper();

    @Override
    public long extractTimestamp(String element, long recordTimestamp) {
      Entity effectiveJava = null;
      try {
        effectiveJava = mapper.readValue(element, Entity.class);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
      return effectiveJava.timestamp();
    }
  }

  static class Entity {

    private long timestamp;

    public Entity() {
    }

    public Entity(long timestamp) {
      this.timestamp = timestamp;
    }

    public long timestamp() {
      return timestamp;
    }

    public void setTimestamp(long timestamp) {
      this.timestamp = timestamp;
    }
  }

}