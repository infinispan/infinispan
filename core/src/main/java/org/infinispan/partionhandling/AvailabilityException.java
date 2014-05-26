package org.infinispan.partionhandling;

import org.infinispan.commons.CacheException;

public class AvailabilityException extends CacheException {
   public AvailabilityException() {
   }

   public AvailabilityException(Throwable cause) {
      super(cause);
   }

   public AvailabilityException(String msg) {
      super(msg);
   }

   public AvailabilityException(String msg, Throwable cause) {
      super(msg, cause);
   }
}
