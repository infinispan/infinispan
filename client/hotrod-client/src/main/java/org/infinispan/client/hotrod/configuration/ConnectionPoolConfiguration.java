package org.infinispan.client.hotrod.configuration;

/**
 * ConnectionPoolConfiguration.
 *
 * @author Tristan Tarrant
 * @since 5.3
 * @deprecated since 15.1, the connection pool is no longer used
 */
@Deprecated(forRemoval = true, since = "15.1")
public class ConnectionPoolConfiguration {
   private final ExhaustedAction exhaustedAction;
   private final int maxActive;
   private final long maxWait;
   private final int minIdle;
   private final long minEvictableIdleTime;
   private final int maxPendingRequests;

   ConnectionPoolConfiguration(ExhaustedAction exhaustedAction, int maxActive, long maxWait, int minIdle, long minEvictableIdleTime, int maxPendingRequests) {
      this.exhaustedAction = exhaustedAction;
      this.maxActive = maxActive;
      this.maxWait = maxWait;
      this.minIdle = minIdle;
      this.minEvictableIdleTime = minEvictableIdleTime;
      this.maxPendingRequests = maxPendingRequests;
   }

   public ExhaustedAction exhaustedAction() {
      return exhaustedAction;
   }

   public int maxActive() {
      return maxActive;
   }

   public long maxWait() {
      return maxWait;
   }

   public int minIdle() {
      return minIdle;
   }

   public long minEvictableIdleTime() {
      return minEvictableIdleTime;
   }

   public int maxPendingRequests() {
      return maxPendingRequests;
   }

   @Override
   public String toString() {
      return "ConnectionPoolConfiguration{" +
            "exhaustedAction=" + exhaustedAction +
            ", maxActive=" + maxActive +
            ", maxWait=" + maxWait +
            ", minIdle=" + minIdle +
            ", minEvictableIdleTime=" + minEvictableIdleTime +
            ", maxPendingRequests=" + maxPendingRequests +
            '}';
   }
}
