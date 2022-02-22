package org.infinispan.api.common.events.cache;

/**
 * TODO SPLIT INTO EMBEDDED/REMOTE
 * @since 14.0
 **/
public class CacheListenerOptions {
   boolean primaryOnly;
   Observation observation;
   boolean clustered;
   boolean includeCurrentState;

   public CacheListenerOptions() {
   }

   public CacheListenerOptions primaryOnly() {
      return primaryOnly(true);
   }

   public CacheListenerOptions primaryOnly(boolean primaryOnly) {
      this.primaryOnly = primaryOnly;
      return this;
   }

   public CacheListenerOptions observation(Observation observation) {
      this.observation = observation;
      return this;
   }

   public CacheListenerOptions clustered() {
      return clustered(true);
   }

   public CacheListenerOptions clustered(boolean clustered) {
      this.clustered = clustered;
      return this;
   }

   public CacheListenerOptions includeCurrentState() {
      return includeCurrentState(true);
   }

   public CacheListenerOptions includeCurrentState(boolean includeCurrentState) {
      this.includeCurrentState = includeCurrentState;
      return this;
   }

   enum Observation {
      PRE() {
         @Override
         public boolean shouldInvoke(boolean pre) {
            return pre;
         }
      },
      POST() {
         @Override
         public boolean shouldInvoke(boolean pre) {
            return !pre;
         }
      },
      BOTH() {
         @Override
         public boolean shouldInvoke(boolean pre) {
            return true;
         }
      };

      public abstract boolean shouldInvoke(boolean pre);
   }
}
