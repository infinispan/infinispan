package org.infinispan.cdc;

import org.infinispan.cdc.internal.configuration.CompleteConfiguration;
import org.infinispan.commons.api.Lifecycle;

public interface ChangeDataCaptureManager extends Lifecycle {

   static ChangeDataCaptureManager create(CompleteConfiguration configuration) {
      // TODO https://github.com/infinispan/infinispan/issues/13538
      return new ChangeDataCaptureManager() {
         @Override
         public void start() { }

         @Override
         public void stop() { }
      };
   }
}
