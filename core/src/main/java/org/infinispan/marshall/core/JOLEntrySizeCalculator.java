package org.infinispan.marshall.core;

import org.infinispan.commons.util.EntrySizeCalculator;
import org.openjdk.jol.info.GraphLayout;

public class JOLEntrySizeCalculator<K, V> implements EntrySizeCalculator<K, V> {

   private static final JOLEntrySizeCalculator<?, ?> INSTANCE = new JOLEntrySizeCalculator<>();

   private JOLEntrySizeCalculator() { }

   @SuppressWarnings("unchecked")
   public static <K, V> JOLEntrySizeCalculator<K, V> getInstance() {
      return (JOLEntrySizeCalculator<K, V>) INSTANCE;
   }

   @Override
   public long calculateSize(K key, V value) {
      return GraphLayout.parseInstance(key, value).totalSize();
   }

   public long deepSizeOf(V value) {
      return GraphLayout.parseInstance(value).totalSize();
   }
}
