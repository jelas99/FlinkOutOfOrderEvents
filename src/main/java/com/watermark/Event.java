package com.watermark;

import java.io.Serializable;

public class Event implements Comparable<Event>, Serializable {

  private String id;
  private Long timestamp;

  public Event(String id, Long timestamp) {
    this.id = id;
    this.timestamp = timestamp;
  }

  public Event() {
  }

  public String id() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public Long timestamp() {
    return timestamp;
  }

  public void setTimestamp(Long timestamp) {
    this.timestamp = timestamp;
  }

  @Override
  public int compareTo(Event o) {
    return timestamp.compareTo(o.timestamp);
  }


  @Override
  public String toString() {
    return "Event{" +
        "id='" + id + '\'' +
        ", timestamp=" + timestamp +
        '}';
  }
}