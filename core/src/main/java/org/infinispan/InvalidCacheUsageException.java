package org.infinispan;

import org.infinispan.commons.CacheException;

/**
 * Thrown when client makes cache usage errors. Situations like this include
 * when clients invoke operations on the cache that are not allowed.
 *
 * @author Galder Zamarreño
 * @since 5.2
 */
public class InvalidCacheUsageException extends CacheException {

   public InvalidCacheUsageException(Throwable cause) {
      super(cause);
   }

   public InvalidCacheUsageException(String msg) {
      super(msg);
   }

   public InvalidCacheUsageException(String msg, Throwable cause) {
      super(msg, cause);
   }

}
