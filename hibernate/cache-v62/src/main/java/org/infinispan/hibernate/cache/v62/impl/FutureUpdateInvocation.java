package org.infinispan.hibernate.cache.v62.impl;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

import org.infinispan.commons.util.Util;
import org.infinispan.functional.FunctionalMap;
import org.infinispan.hibernate.cache.commons.InfinispanDataRegion;
import org.infinispan.hibernate.cache.commons.util.FutureUpdate;
import org.infinispan.hibernate.cache.commons.util.InfinispanMessageLogger;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class FutureUpdateInvocation implements Invocation, BiFunction<Void, Throwable, Void> {
   private static final InfinispanMessageLogger log = InfinispanMessageLogger.Provider.getLog(FutureUpdateInvocation.class);

   private final UUID uuid = Util.threadLocalRandomUUID();
   private final Object key;
   private final Object value;
   private final InfinispanDataRegion region;
   private final long sessionTimestamp;
   private final FunctionalMap.ReadWriteMap<Object, Object> rwMap;
   private final CompletableFuture<Void> future = new CompletableFuture<>();

   public FutureUpdateInvocation(FunctionalMap.ReadWriteMap<Object, Object> rwMap,
         Object key, Object value, InfinispanDataRegion region, long sessionTimestamp) {

      this.rwMap = rwMap;
      this.key = key;
      this.value = value;
      this.region = region;
      this.sessionTimestamp = sessionTimestamp;
   }

   public UUID getUuid() {
      return uuid;
   }

   @Override
   public CompletableFuture<Void> invoke(boolean success) {
      // If the region was invalidated during this session, we can't know that the value we're inserting is valid
      // so we'll just null the tombstone
      if (sessionTimestamp < region.getLastRegionInvalidation()) {
         success = false;
      }
      // Exceptions in #afterCompletion() are silently ignored, since the transaction
      // is already committed in DB. However we must not return until we update the cache.
      FutureUpdate futureUpdate = new FutureUpdate(uuid, region.nextTimestamp(), success ? this.value : null);
      for (; ; ) {
         try {
            // We expect that when the transaction completes further reads from cache will return the updated value.
            // UnorderedDistributionInterceptor makes sure that the update is executed on the node first, and here
            // we're waiting for the local update. The remote update does not concern us - the cache is async and
            // we won't wait for that.
            rwMap.eval(key, futureUpdate).handle(this);
            return future;
         } catch (Exception e) {
            log.failureInAfterCompletion(e);
         }
      }
   }

   @Override
   public Void apply(Void nil, Throwable throwable) {
      if (throwable != null) {
         log.failureInAfterCompletion(throwable);
         invoke(false);
      } else {
         future.complete(null);
      }
      return null;
   }
}
