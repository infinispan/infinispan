package org.horizon.eviction.algorithms;

import org.horizon.eviction.EvictionQueue;

public abstract class BaseEvictionQueue implements EvictionQueue {

   public boolean isEmpty() {
      return size() == 0;
   }

   public void visit(Object key) {
      // default impl to ignore this
   }
}
