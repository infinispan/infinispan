package org.infinispan.client.hotrod.impl.operations;

public class TimeoutHotRodOperation<T> extends DelegatingHotRodOperation<T> {
   private final long timeoutMillis;

   public TimeoutHotRodOperation(HotRodOperation<T> delegate, long timeoutMillis) {
      super(delegate);
      this.timeoutMillis = timeoutMillis;
   }

   @Override
   public long timeout() {
      return timeoutMillis;
   }
}
