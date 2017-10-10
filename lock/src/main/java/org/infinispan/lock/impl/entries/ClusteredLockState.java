package org.infinispan.lock.impl.entries;

/**
 * Enum that represents the state of the lock.
 * Currently, two states are supported : {@link ClusteredLockState#ACQUIRED} and {@link ClusteredLockState#RELEASED}
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
public enum ClusteredLockState {
   ACQUIRED,
   RELEASED;

   private static final ClusteredLockState[] CACHED_VALUES = ClusteredLockState.values();

   public static ClusteredLockState valueOf(int index) {
      return CACHED_VALUES[index];
   }
}
