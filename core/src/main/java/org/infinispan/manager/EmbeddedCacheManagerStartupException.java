package org.infinispan.manager;

import org.infinispan.commons.CacheException;

/**
 * An exception to encapsulate an error when starting up a cache manager
 *
 * @author Manik Surtani
 * @since 4.2.2
 */
public class EmbeddedCacheManagerStartupException extends CacheException {
   public EmbeddedCacheManagerStartupException() {
   }

   public EmbeddedCacheManagerStartupException(Throwable cause) {
      super(cause);
   }

   public EmbeddedCacheManagerStartupException(String msg) {
      super(msg);
   }

   public EmbeddedCacheManagerStartupException(String msg, Throwable cause) {
      super(msg, cause);
   }
}
