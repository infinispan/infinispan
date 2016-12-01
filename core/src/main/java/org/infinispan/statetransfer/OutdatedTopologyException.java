package org.infinispan.statetransfer;

import org.infinispan.commons.CacheException;

/**
 * An exception signalling that a command should be retried because it was executed with an outdated
 * topology.
 * <p>
 * This can happen for non-tx caches, if the primary owner doesn't respond (either because it left the
 * cluster or because this particular cache is no longer running).
 *
 * @author Dan Berindei
 * @since 6.0
 */
public class OutdatedTopologyException extends CacheException {
   public final int requestedTopologyId;

   /**
    * A cached instance of {@link OutdatedTopologyException}.
    * <p>
    * This exception has not stack trace information and it should be used internally to notify a topology change while
    * handle a command.
    * <p>
    * It avoids the cost associated to create and collect the stack when it isn't needed.
    *
    * @return a cached instance of {@link OutdatedTopologyException}.
    */
   @SuppressWarnings("ThrowableInstanceNeverThrown")
   public static final OutdatedTopologyException INSTANCE = new OutdatedTopologyException();

   private OutdatedTopologyException() {
      super("Topology changed while handling command", null, false, false);
      requestedTopologyId = -1;
   }

   public OutdatedTopologyException(String msg) {
      super(msg, null, false, false);
      requestedTopologyId = -1;
   }

   /**
    * Request retrying the command in explicitly set topology (or later one).
    * @param requestedTopologyId
    */
   public OutdatedTopologyException(int requestedTopologyId) {
      super(null, null, false, false);
      this.requestedTopologyId = requestedTopologyId;
   }
}
