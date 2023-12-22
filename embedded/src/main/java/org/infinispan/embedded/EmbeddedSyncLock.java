package org.infinispan.embedded;

import static org.infinispan.commons.util.concurrent.CompletableFutures.uncheckedAwait;

import java.util.concurrent.TimeUnit;

import org.infinispan.api.sync.SyncContainer;
import org.infinispan.api.sync.SyncLock;
import org.infinispan.lock.api.ClusteredLock;

/**
 * @since 15.0
 */
public class EmbeddedSyncLock implements SyncLock {
   private final Embedded embedded;
   private final String name;
   private final ClusteredLock lock;

   EmbeddedSyncLock(Embedded embedded, String name, ClusteredLock lock) {
      this.embedded = embedded;
      this.name = name;
      this.lock = lock;
   }

   @Override
   public String name() {
      ;
      return name;
   }

   @Override
   public SyncContainer container() {
      return embedded.sync();
   }

   @Override
   public void lock() {
      uncheckedAwait(lock.lock());
   }

   @Override
   public boolean tryLock() {
      return uncheckedAwait(lock.tryLock());
   }

   @Override
   public boolean tryLock(long time, TimeUnit unit) {
      return uncheckedAwait(lock.tryLock(time, unit));
   }

   @Override
   public void unlock() {
      uncheckedAwait(lock.unlock());
   }

   @Override
   public boolean isLocked() {
      return uncheckedAwait(lock.isLocked());
   }

   @Override
   public boolean isLockedByMe() {
      return uncheckedAwait(lock.isLockedByMe());
   }
}
