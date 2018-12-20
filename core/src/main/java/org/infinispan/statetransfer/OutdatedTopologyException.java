package org.infinispan.statetransfer;

import org.infinispan.commons.CacheException;

/**
 * An exception signalling that a command should be retried because a newer topology was seen during execution.
 *
 * <p>Most of the time, read commands can be retried in the same topology, so they use a delta of 0,
 * see {@link #RETRY_SAME_TOPOLOGY}.
 * Write commands cannot be retried in the same topology, so they always use a delta of 1 (or more).</p>
 *
 * <p>This exception can be thrown very often when node is joining or leaving, so it has not stack trace information,
 * and using the constants is preferred.</p>
 *
 * @author Dan Berindei
 * @since 6.0
 */
public class OutdatedTopologyException extends CacheException {
   private static final long serialVersionUID = -7405935610562980779L;

   public final int topologyIdDelta;

   /**
    * A cached instance that requests the command's topology id + 1.
    */
   public static final OutdatedTopologyException RETRY_NEXT_TOPOLOGY =
      new OutdatedTopologyException("Retry in the next topology", 1);

   /**
    * A cached instance, used for read commands that need to be retried in the same topology.
    *
    * <p>This happens because we read from backup owners when the primary owners no longer have the entry,
    * so we only retry when all of the owners reply with an UnsureResponse.
    * Topologies T and T+1 always have at least one read owner in common, so receiving UnsureResponse from all the
    * owners means either one owner had topology T+2 and by now we have at least T+1, or one owner had topology T-1
    * and another had T+1, and by now all should have at least T.</p>
    */
   public static final OutdatedTopologyException RETRY_SAME_TOPOLOGY =
      new OutdatedTopologyException("Retry command in the same topology", 0);

   private OutdatedTopologyException(String message, int topologyIdDelta) {
      super(message, null, false, false);
      this.topologyIdDelta = topologyIdDelta;
   }

   /**
    * Request the next topology (delta = 1) and use a custom message.
    *
    * @deprecated Since 10.0, please use the constants
    */
   @Deprecated
   public OutdatedTopologyException(String msg) {
      super(msg, null, false, false);
      this.topologyIdDelta = 1;
   }

   /**
    * Request retrying the command in explicitly set topology (or later one).
    *
    * @deprecated Since 10.0, the explicit topology is ignored and the delta is set to 1
    */
   @Deprecated
   public OutdatedTopologyException(int topologyIdDelta) {
      this(null, 1);
   }
}
