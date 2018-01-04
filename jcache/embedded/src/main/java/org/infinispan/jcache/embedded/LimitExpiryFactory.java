package org.infinispan.jcache.embedded;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import javax.cache.configuration.Factory;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;

public class LimitExpiryFactory implements Factory<ExpiryPolicy>, Serializable {
   private final Factory<ExpiryPolicy> factory;
   private final long lifespan;
   private final long maxIdle;

   public LimitExpiryFactory(Factory<ExpiryPolicy> factory, long lifespan, long maxIdle) {
      this.factory = factory;
      this.lifespan = lifespan;
      this.maxIdle = maxIdle;
   }

   @Override
   public ExpiryPolicy create() {
      return new LimitExpiryPolicy(factory.create(), lifespan, maxIdle);
   }

   private static class LimitExpiryPolicy implements ExpiryPolicy {
      private final ExpiryPolicy expiryPolicy;
      private final long maxIdle;
      private final long min;

      public LimitExpiryPolicy(ExpiryPolicy expiryPolicy, long lifespan, long maxIdle) {
         this.expiryPolicy = expiryPolicy;
         this.maxIdle = maxIdle;
         this.min = maxIdle >= 0 ? (lifespan >= 0 ? Math.min(maxIdle, lifespan) : maxIdle) : lifespan;
         assert min >= 0;
      }

      @Override
      public Duration getExpiryForCreation() {
         Duration duration = expiryPolicy.getExpiryForCreation();
         if (duration.isZero()) return duration;
         if (duration.isEternal()) return new Duration(TimeUnit.MILLISECONDS, min);
         long actualMin = Math.min(duration.getTimeUnit().toMillis(duration.getDurationAmount()), this.min);
         return new Duration(TimeUnit.MILLISECONDS, actualMin);
      }

      @Override
      public Duration getExpiryForAccess() {
         Duration duration = expiryPolicy.getExpiryForAccess();
         if (duration.isZero() || maxIdle < 0) return duration;
         if (duration.isEternal()) return new Duration(TimeUnit.MILLISECONDS, maxIdle);
         long actualMin = Math.min(duration.getTimeUnit().toMillis(duration.getDurationAmount()), this.maxIdle);
         return new Duration(TimeUnit.MILLISECONDS, actualMin);
      }

      @Override
      public Duration getExpiryForUpdate() {
         Duration duration = expiryPolicy.getExpiryForUpdate();
         if (duration.isZero()) return duration;
         if (duration.isEternal()) return new Duration(TimeUnit.MILLISECONDS, min);
         long actualMin = Math.min(duration.getTimeUnit().toMillis(duration.getDurationAmount()), this.min);
         return new Duration(TimeUnit.MILLISECONDS, actualMin);
      }
   }
}
