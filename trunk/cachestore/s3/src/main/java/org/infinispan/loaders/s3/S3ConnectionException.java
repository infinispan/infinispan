package org.infinispan.loaders.s3;

import org.infinispan.loaders.CacheLoaderException;

/**
 * An exception thrown by a {@link S3Bucket} implementation if there are problems reading from S3.
 *
 * @author Adrian Cole
 * @since 4.0
 */
public class S3ConnectionException extends CacheLoaderException {
   public S3ConnectionException() {
   }

   public S3ConnectionException(String message) {
      super(message);
   }

   public S3ConnectionException(String message, Throwable cause) {
      super(message, cause);
   }

   public S3ConnectionException(Throwable cause) {
      super(cause);
   }
}