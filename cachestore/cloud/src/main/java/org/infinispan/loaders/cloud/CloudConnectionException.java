package org.infinispan.loaders.cloud;

import org.infinispan.loaders.CacheLoaderException;

/**
 * An exception thrown by the {@link org.infinispan.loaders.cloud.CloudCacheStore} if there are problems connecting
 * to the cloud storage provider.
 *
 * @author Adrian Cole
 * @author Manik Surtani
 * @since 4.0
 */
public class CloudConnectionException extends CacheLoaderException {
   public CloudConnectionException() {
   }

   public CloudConnectionException(String message) {
      super(message);
   }

   public CloudConnectionException(String message, Throwable cause) {
      super(message, cause);
   }

   public CloudConnectionException(Throwable cause) {
      super(cause);
   }
}