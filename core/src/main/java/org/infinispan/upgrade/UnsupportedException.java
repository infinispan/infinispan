package org.infinispan.upgrade;

/**
 * Exception thrown when an action is attempted before all members of the cluster support it.
 * This can happen during a rolling upgrade of the cluster, e.g. when a new command is first introduced. The Exception will
 * continue to be thrown until all cluster cluster members have a {@link ManagerVersion} greater
 * or equal to {@link VersionAware#supportedSince()} value.
 * <p>
 * If the unsupported functionality is required, then an appropriate backup/retry policy should be
 * implemented to retry to the command when the cluster is ready. For new endpoint functionality, e.g. a new REST URI
 * or RESP command, then an appropriate failure response should be transmitted to the user indicating this failure is
 * temprorary.
 *
 * @since 16.0
 */
public class UnsupportedException extends RuntimeException {
   public UnsupportedException(String message) {
      super(message);
   }
}
