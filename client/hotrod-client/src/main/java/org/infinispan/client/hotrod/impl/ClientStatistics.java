package org.infinispan.client.hotrod.impl;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.infinispan.client.hotrod.jmx.RemoteCacheClientStatisticsMXBean;
import org.infinispan.client.hotrod.near.NearCacheService;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.concurrent.StripedCounters;

public class ClientStatistics implements RemoteCacheClientStatisticsMXBean {
   private final boolean enabled;
   private final AtomicLong startNanoseconds = new AtomicLong(0);
   private final AtomicLong resetNanoseconds = new AtomicLong(0);
   private final NearCacheService nearCacheService;
   private final TimeService timeService;
   private StripedCounters<StripeB> counters = new StripedCounters<>(StripeC::new);

   ClientStatistics(boolean enabled, TimeService timeService, NearCacheService nearCacheService) {
      this.enabled = enabled;
      this.timeService = timeService;
      this.nearCacheService = nearCacheService;
      if (nearCacheService != null)
         nearCacheService.setInvalidationCallback(() -> incrementNearCacheInvalidations());
   }

   ClientStatistics(boolean enabled, TimeService timeService) {
      this(enabled, timeService, null);
   }

   public boolean isEnabled() {
      return enabled;
   }

   @Override
   public long getRemoteHits() {
      return counters.get(StripeB.remoteCacheHitsFieldUpdater);
   }

   @Override
   public long getRemoteMisses() {
      return counters.get(StripeB.remoteCacheMissesFieldUpdater);
   }

   @Override
   public long getAverageRemoteReadTime() {
      long total = counters.get(StripeB.remoteCacheHitsFieldUpdater) + counters.get(StripeB.remoteCacheMissesFieldUpdater);
      if (total == 0)
         return 0;
      total = (counters.get(StripeB.remoteCacheHitsTimeFieldUpdater) + counters.get(StripeB.remoteCacheMissesTimeFieldUpdater)) / total;
      return TimeUnit.NANOSECONDS.toMillis(total);
   }

   @Override
   public long getRemoteStores() {
      return counters.get(StripeB.remoteCacheStoresFieldUpdater);
   }

   @Override
   public long getAverageRemoteStoreTime() {
      long total = counters.get(StripeB.remoteCacheStoresFieldUpdater);
      if (total == 0)
         return 0;
      total = counters.get(StripeB.remoteCacheStoresTimeFieldUpdater) / total;
      return TimeUnit.NANOSECONDS.toMillis(total);
   }

   @Override
   public long getRemoteRemoves() {
      return counters.get(StripeB.remoteCacheRemovesFieldUpdater);
   }

   @Override
   public long getAverageRemoteRemovesTime() {
      long total = counters.get(StripeB.remoteCacheRemovesFieldUpdater);
      if (total == 0)
         return 0;
      total = counters.get(StripeB.remoteCacheRemovesTimeFieldUpdater) / total;
      return TimeUnit.NANOSECONDS.toMillis(total);
   }

   @Override
   public long getNearCacheHits() {
      return counters.get(StripeB.nearCacheHitsFieldUpdater);
   }

   @Override
   public long getNearCacheMisses() {
      return counters.get(StripeB.nearCacheMissesFieldUpdater);
   }

   @Override
   public long getNearCacheInvalidations() {
      return counters.get(StripeB.nearCacheInvalidationsFieldUpdater);
   }

   @Override
   public long getNearCacheSize() {
      return nearCacheService != null ? nearCacheService.size() : 0;
   }

   public long time() {
      return timeService.time();
   }

   public void dataRead(boolean foundValue, long startTimeNanoSeconds, int count) {
      long duration = timeService.timeDuration(startTimeNanoSeconds, TimeUnit.MILLISECONDS);
      StripeB stripe = counters.stripeForCurrentThread();
      if (foundValue) {
         counters.add(StripeB.remoteCacheHitsTimeFieldUpdater, stripe, duration);
         counters.add(StripeB.remoteCacheHitsFieldUpdater, stripe, count);
      } else {
         counters.add(StripeB.remoteCacheMissesTimeFieldUpdater, stripe, duration);
         counters.add(StripeB.remoteCacheMissesFieldUpdater, stripe, count);
      }
   }

   public void dataStore(long startTimeNanoSeconds, int count) {
      long duration = timeService.timeDuration(startTimeNanoSeconds, TimeUnit.MILLISECONDS);
      StripeB stripe = counters.stripeForCurrentThread();
      counters.add(StripeB.remoteCacheStoresTimeFieldUpdater, stripe, duration);
      counters.add(StripeB.remoteCacheStoresFieldUpdater, stripe, count);
   }

   public void dataRemove(long startTimeNanoSeconds, int count) {
      long duration = timeService.timeDuration(startTimeNanoSeconds, TimeUnit.MILLISECONDS);
      StripeB stripe = counters.stripeForCurrentThread();
      counters.add(StripeB.remoteCacheRemovesTimeFieldUpdater, stripe, duration);
      counters.add(StripeB.remoteCacheRemovesFieldUpdater, stripe, count);
   }

   public void incrementNearCacheMisses() {
      counters.increment(StripeB.nearCacheMissesFieldUpdater, counters.stripeForCurrentThread());
   }

   public void incrementNearCacheHits() {
      counters.increment(StripeB.nearCacheHitsFieldUpdater, counters.stripeForCurrentThread());
   }

   public void incrementNearCacheInvalidations() {
      counters.increment(StripeB.nearCacheInvalidationsFieldUpdater, counters.stripeForCurrentThread());
   }

   @Override
   public void resetStatistics() {
      counters.reset(StripeB.remoteCacheHitsFieldUpdater);
      counters.reset(StripeB.remoteCacheHitsTimeFieldUpdater);
      counters.reset(StripeB.remoteCacheMissesFieldUpdater);
      counters.reset(StripeB.remoteCacheMissesTimeFieldUpdater);
      counters.reset(StripeB.remoteCacheRemovesFieldUpdater);
      counters.reset(StripeB.remoteCacheRemovesTimeFieldUpdater);
      counters.reset(StripeB.remoteCacheStoresFieldUpdater);
      counters.reset(StripeB.remoteCacheStoresTimeFieldUpdater);
      counters.reset(StripeB.nearCacheHitsFieldUpdater);
      counters.reset(StripeB.nearCacheMissesFieldUpdater);
      counters.reset(StripeB.nearCacheInvalidationsFieldUpdater);

      startNanoseconds.set(timeService.time());
      resetNanoseconds.set(startNanoseconds.get());
   }

   @Override
   public long getTimeSinceReset() {
      return timeService.timeDuration(resetNanoseconds.get(), TimeUnit.SECONDS);
   }

   @SuppressWarnings("unused")
   private static class StripeA {
      private long slack1, slack2, slack3, slack4, slack5, slack6, slack7, slack8;
   }

   @SuppressWarnings({"unused", "VolatileLongOrDoubleField"})
   private static class StripeB extends StripeA {
      static final AtomicLongFieldUpdater<StripeB> remoteCacheHitsFieldUpdater =
            AtomicLongFieldUpdater.newUpdater(StripeB.class, "remoteCacheHits");
      static final AtomicLongFieldUpdater<StripeB> remoteCacheHitsTimeFieldUpdater =
            AtomicLongFieldUpdater.newUpdater(StripeB.class, "remoteCacheHitsTime");
      static final AtomicLongFieldUpdater<StripeB> remoteCacheMissesFieldUpdater =
            AtomicLongFieldUpdater.newUpdater(StripeB.class, "remoteCacheMisses");
      static final AtomicLongFieldUpdater<StripeB> remoteCacheMissesTimeFieldUpdater =
            AtomicLongFieldUpdater.newUpdater(StripeB.class, "remoteCacheMissesTime");
      static final AtomicLongFieldUpdater<StripeB> remoteCacheRemovesFieldUpdater =
            AtomicLongFieldUpdater.newUpdater(StripeB.class, "remoteCacheRemoves");
      static final AtomicLongFieldUpdater<StripeB> remoteCacheRemovesTimeFieldUpdater =
            AtomicLongFieldUpdater.newUpdater(StripeB.class, "remoteCacheRemovesTime");
      static final AtomicLongFieldUpdater<StripeB> remoteCacheStoresFieldUpdater =
            AtomicLongFieldUpdater.newUpdater(StripeB.class, "remoteCacheStores");
      static final AtomicLongFieldUpdater<StripeB> remoteCacheStoresTimeFieldUpdater =
            AtomicLongFieldUpdater.newUpdater(StripeB.class, "remoteCacheStoresTime");
      static final AtomicLongFieldUpdater<StripeB> nearCacheHitsFieldUpdater =
            AtomicLongFieldUpdater.newUpdater(StripeB.class, "nearCacheHits");
      static final AtomicLongFieldUpdater<StripeB> nearCacheMissesFieldUpdater =
            AtomicLongFieldUpdater.newUpdater(StripeB.class, "nearCacheMisses");
      static final AtomicLongFieldUpdater<StripeB> nearCacheInvalidationsFieldUpdater =
            AtomicLongFieldUpdater.newUpdater(StripeB.class, "nearCacheInvalidations");

      private volatile long remoteCacheHits = 0;
      private volatile long remoteCacheHitsTime = 0;
      private volatile long remoteCacheMisses = 0;
      private volatile long remoteCacheMissesTime = 0;
      private volatile long remoteCacheRemoves = 0;
      private volatile long remoteCacheRemovesTime = 0;
      private volatile long remoteCacheStores = 0;
      private volatile long remoteCacheStoresTime = 0;
      private volatile long nearCacheHits = 0;
      private volatile long nearCacheMisses = 0;
      private volatile long nearCacheInvalidations = 0;
   }

   @SuppressWarnings("unused")
   private static class StripeC extends StripeB {
      private long slack1, slack2, slack3, slack4, slack5, slack6, slack7, slack8;
   }
}
