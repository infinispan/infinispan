package org.infinispan.interceptors.distribution;

import org.infinispan.commons.CacheException;

/**
 * Thrown when the version of entry has changed between loading the entry to the context and committing new value.
 * @see ScatteringInterceptor
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class ConcurrentChangeException extends CacheException {

   /**
    * Throwing this exception is cheaper because it does not fill in the stack trace.
    */
   public ConcurrentChangeException() {
      super(null, null, false, false);
   }
}
