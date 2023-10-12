package org.infinispan.telemetry;

public interface SafeAutoClosable extends AutoCloseable {

   @Override
   void close();

}
