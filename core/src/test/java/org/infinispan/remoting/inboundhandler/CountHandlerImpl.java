package org.infinispan.remoting.inboundhandler;

import java.util.concurrent.atomic.AtomicInteger;

final class CountHandlerImpl implements CountHandler{

   private final AtomicInteger count = new AtomicInteger(0);

   @Override
   public int sum() {
      return count.get();
   }

   @Override
   public void reset() {
      count.set(0);
   }

   void increment() {
      count.incrementAndGet();
   }
}
