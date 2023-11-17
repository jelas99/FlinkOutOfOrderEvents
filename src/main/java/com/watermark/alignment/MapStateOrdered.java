package com.watermark.alignment;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.flink.api.common.state.MapState;

public class MapStateOrdered <K, V> implements MapState<K, V> {

    private final MapState<K, V> originalState;

    private final Map<K, V> emptyState = Collections.<K, V>emptyMap();

    MapStateOrdered(MapState<K, V> originalState) {
      this.originalState = originalState;
    }

    // ------------------------------------------------------------------------

    @Override
    public V get(K key) throws Exception {
      return originalState.get(key);
    }

    @Override
    public void put(K key, V value) throws Exception {
      originalState.put(key, value);
    }

    @Override
    public void putAll(Map<K, V> value) throws Exception {
      originalState.putAll(value);
    }

    @Override
    public void clear() {
      originalState.clear();
    }

    @Override
    public void remove(K key) throws Exception {
      originalState.remove(key);
    }

    @Override
    public boolean contains(K key) throws Exception {
      return originalState.contains(key);
    }

    @Override
    public Iterable<Map.Entry<K, V>> entries() throws Exception {
      Iterable<Map.Entry<K, V>> original = originalState.entries();
      return original != null ? original : emptyState.entrySet();
    }

    @Override
    public Iterable<K> keys() throws Exception {
      Iterable<K> original = originalState.keys();
      return original != null ? original : emptyState.keySet();
    }

    @Override
    public Iterable<V> values() throws Exception {
      Iterable<V> original = originalState.values();
      return original != null ? original : emptyState.values();
    }

    @Override
    public Iterator<Entry<K, V>> iterator() throws Exception {
      Iterator<Map.Entry<K, V>> original = originalState.iterator();
      return original != null ? original : emptyState.entrySet().iterator();
    }

    @Override
    public boolean isEmpty() throws Exception {
      return originalState.isEmpty();
    }
  }
