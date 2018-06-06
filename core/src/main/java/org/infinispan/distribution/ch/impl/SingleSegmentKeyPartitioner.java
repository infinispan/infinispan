package org.infinispan.distribution.ch.impl;

import org.infinispan.distribution.ch.KeyPartitioner;

/**
 * KeyPartitioner that always returns 0 for a given segment. This can be useful when segments are not in use, such
 * as local or invalidation caches.
 * @author wburns
 * @since 9.3
 */
public class SingleSegmentKeyPartitioner implements KeyPartitioner {
   private SingleSegmentKeyPartitioner() { }

   private static final SingleSegmentKeyPartitioner INSTANCE = new SingleSegmentKeyPartitioner();

   public static SingleSegmentKeyPartitioner getInstance() {
      return INSTANCE;
   }

   @Override
   public int getSegment(Object key) {
      return 0;
   }
}
