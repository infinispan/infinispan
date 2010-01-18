package org.infinispan.loaders.cloud;

import org.infinispan.loaders.CacheLoaderException;

public class CloudConnectionException extends CacheLoaderException {
   public CloudConnectionException() {
   }

   public CloudConnectionException(Throwable cause) {
      super(cause);
   }

   public CloudConnectionException(String message) {
      super(message);
   }

   public CloudConnectionException(String message, Throwable cause) {
      super(message, cause);
   }
}
