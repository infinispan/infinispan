package org.infinispan.embedded;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

import org.infinispan.api.async.AsyncLock;
import org.infinispan.api.async.AsyncLocks;
import org.infinispan.api.configuration.LockConfiguration;
import org.infinispan.lock.EmbeddedClusteredLockManagerFactory;
import org.infinispan.lock.api.ClusteredLockManager;

/**
 * @since 15.0
 */
public class EmbeddedAsyncLocks implements AsyncLocks {
   private final Embedded embedded;
   private final ClusteredLockManager lockManager;

   EmbeddedAsyncLocks(Embedded embedded) {
      this.embedded = embedded;
      this.lockManager = EmbeddedClusteredLockManagerFactory.from(embedded.cacheManager);
   }

   @Override
   public CompletionStage<AsyncLock> create(String name, LockConfiguration configuration) {
      return null;
   }

   @Override
   public CompletionStage<AsyncLock> lock(String name) {
      return null;
   }

   @Override
   public CompletionStage<Void> remove(String name) {
      return null;
   }

   @Override
   public Flow.Publisher<String> names() {
      return null;
   }
}
