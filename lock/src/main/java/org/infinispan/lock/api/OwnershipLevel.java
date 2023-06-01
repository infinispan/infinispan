package org.infinispan.lock.api;

/**
 * Ownership level is a configuration parameter for {@link ClusteredLock}. The level of the ownership scopes the
 * execution of the code once the lock is acquired. If the lock owner is the {@link #NODE}, this means that any thread
 * on the node can release the lock.
 *
 * @author Katia Aresti, karesti@redhat.com
 * @see <a href="http://infinispan.org/documentation/">Infinispan documentation</a>
 * @since 9.2
 */
public enum OwnershipLevel {
   // The owner of the lock is a node. Only the node that owns the Lock can release it
   NODE,
   // The owner of the lock is an instance. Only the instance that owns the lock can release it
   INSTANCE
}
