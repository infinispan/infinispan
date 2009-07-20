package org.infinispan.util.concurrent.locks;

import org.infinispan.CacheException;

/**
 * Exception signaling detected deadlocks.
 *
 * @author Mircea.Markus@jboss.com
 */
public class DeadlockDetectedException extends CacheException {
   public DeadlockDetectedException(String msg) {
      super(msg);
   }
}
