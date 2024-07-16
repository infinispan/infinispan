package org.infinispan.server.test.core;

import org.testcontainers.containers.output.BaseConsumer;
import org.testcontainers.containers.output.OutputFrame;

public class NettyLeakDetectionLoggingConsumer extends BaseConsumer<NettyLeakDetectionLoggingConsumer> {
   private boolean leakDetected;

   public NettyLeakDetectionLoggingConsumer() {
   }

   @Override
   public void accept(OutputFrame outputFrame) {
      String log = outputFrame.getUtf8String();
      if (log.contains(" LEAK:")) {
         leakDetected = true;
      }
   }

   public boolean leakDetected() {
      return leakDetected;
   }
}
