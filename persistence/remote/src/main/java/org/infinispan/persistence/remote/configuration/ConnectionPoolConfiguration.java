package org.infinispan.persistence.remote.configuration;

public class ConnectionPoolConfiguration {
   private final ExhaustedAction exhaustedAction;
   private final int maxActive;
   private final int maxTotal;
   private final int maxIdle;
   private final int minIdle;
   private final long timeBetweenEvictionRuns;
   private final long minEvictableIdleTime;
   private final boolean testWhileIdle;

   ConnectionPoolConfiguration(ExhaustedAction exhaustedAction, int maxActive, int maxTotal, int maxIdle, int minIdle,
         long timeBetweenEvictionRuns, long minEvictableIdleTime, boolean testWhileIdle) {
      this.exhaustedAction = exhaustedAction;
      this.maxActive = maxActive;
      this.maxTotal = maxTotal;
      this.maxIdle = maxIdle;
      this.minIdle = minIdle;
      this.timeBetweenEvictionRuns = timeBetweenEvictionRuns;
      this.minEvictableIdleTime = minEvictableIdleTime;
      this.testWhileIdle = testWhileIdle;
   }

   public ExhaustedAction exhaustedAction() {
      return exhaustedAction;
   }

   public int maxActive() {
      return maxActive;
   }

   public int maxTotal() {
      return maxTotal;
   }

   public int maxIdle() {
      return maxIdle;
   }

   public int minIdle() {
      return minIdle;
   }

   public long timeBetweenEvictionRuns() {
      return timeBetweenEvictionRuns;
   }

   public long minEvictableIdleTime() {
      return minEvictableIdleTime;
   }

   public boolean testWhileIdle() {
      return testWhileIdle;
   }

   @Override
   public String toString() {
      return "ConnectionPoolConfiguration [exhaustedAction=" + exhaustedAction + ", maxActive=" + maxActive
            + ", maxTotal=" + maxTotal + ", maxIdle=" + maxIdle + ", minIdle=" + minIdle + ", timeBetweenEvictionRuns="
            + timeBetweenEvictionRuns + ", minEvictableIdleTime=" + minEvictableIdleTime + ", testWhileIdle="
            + testWhileIdle + "]";
   }

}
