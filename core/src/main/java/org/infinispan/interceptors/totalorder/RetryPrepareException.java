package org.infinispan.interceptors.totalorder;

import org.infinispan.commons.CacheException;

/**
 * Indicates the state transfer is running and the prepare should be retried.
 *
 * @author Pedro Ruivo
 * @since 5.3
 */
public class RetryPrepareException extends CacheException {

   public RetryPrepareException() {
      super();
   }

}
