package org.infinispan.telemetry;

public interface InfinispanSpan {

   SafeAutoClosable makeCurrent();

   void complete();

   void recordException(Throwable throwable);

}
