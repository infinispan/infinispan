package org.infinispan.commons;

/**
 * This exception is thrown when the cache or cache manager does not have the right lifecycle state for cross-site
 * operations to be called on it. Situations like this include when the cache is stopping or is stopped, when the cache
 * manager is stopped...etc.
 *
 * @since 15
 */
public class CrossSiteIllegalLifecycleStateException extends CacheException {
   public CrossSiteIllegalLifecycleStateException() {
   }

   public CrossSiteIllegalLifecycleStateException(String s) {
      super(s);
   }

   public CrossSiteIllegalLifecycleStateException(String message, Throwable cause) {
      super(message, cause);
   }

   public CrossSiteIllegalLifecycleStateException(Throwable cause) {
      super(cause.getMessage(), cause);
   }
}
