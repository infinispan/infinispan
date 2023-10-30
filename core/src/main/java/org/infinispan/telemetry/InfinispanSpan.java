package org.infinispan.telemetry;

public interface InfinispanSpan {

   AutoCloseable makeCurrent();

   void complete();

   void recordException(Throwable throwable);

}
