package org.infinispan.hibernate.cache.commons.util;

/**
 * Called by {@link org.infinispan.hibernate.cache.commons.access.LockingInterceptor} when the function
 * was applied locally (after unlocking the entry and we're just waiting for the replication.
 * Note: we don't mind if the command is retried, the important part is that it was already applied locally.
 */
public interface CompletableFunction {
   boolean isComplete();
   void markComplete();
}
