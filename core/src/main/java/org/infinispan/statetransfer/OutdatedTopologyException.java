package org.infinispan.statetransfer;

import org.infinispan.commons.CacheException;

/**
 * An exception signalling that a command should be retried because it was executed with an outdated
 * topology.
 *
 * This can happen for non-tx caches, if the primary owner doesn't respond (either because it left the
 * cluster or because this particular cache is no longer running).
 *
 * @author Dan Berindei
 * @since 6.0
 */
public class OutdatedTopologyException extends CacheException {
   public OutdatedTopologyException(String msg) {
      super(msg);
   }

   public OutdatedTopologyException(String msg, Throwable cause) {
      super(msg, cause);
   }
}
