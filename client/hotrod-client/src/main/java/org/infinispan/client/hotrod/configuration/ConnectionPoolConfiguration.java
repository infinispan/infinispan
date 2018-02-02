package org.infinispan.client.hotrod.configuration;

/**
 * ConnectionPoolConfiguration.
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
public class ConnectionPoolConfiguration {
   private final ExhaustedAction exhaustedAction;
   private final boolean lifo;
   private final int maxActive;
   private final int maxTotal;
   private final long maxWait;
   private final int maxIdle;
   private final int minIdle;
   private final int numTestsPerEvictionRun;
   private final long timeBetweenEvictionRuns;
   private final long minEvictableIdleTime;
   private final boolean testOnBorrow;
   private final boolean testOnReturn;
   private final boolean testWhileIdle;
   private final int maxPendingRequests;

   ConnectionPoolConfiguration(ExhaustedAction exhaustedAction, boolean lifo, int maxActive, int maxTotal, long maxWait, int maxIdle, int minIdle, int numTestsPerEvictionRun,
                               long timeBetweenEvictionRuns, long minEvictableIdleTime, boolean testOnBorrow, boolean testOnReturn, boolean testWhileIdle, int maxPendingRequests) {
      this.exhaustedAction = exhaustedAction;
      this.lifo = lifo;
      this.maxActive = maxActive;
      this.maxTotal = maxTotal;
      this.maxWait = maxWait;
      this.maxIdle = maxIdle;
      this.minIdle = minIdle;
      this.numTestsPerEvictionRun = numTestsPerEvictionRun;
      this.timeBetweenEvictionRuns = timeBetweenEvictionRuns;
      this.minEvictableIdleTime = minEvictableIdleTime;
      this.testOnBorrow = testOnBorrow;
      this.testOnReturn = testOnReturn;
      this.testWhileIdle = testWhileIdle;
      this.maxPendingRequests = maxPendingRequests;
   }

   public ExhaustedAction exhaustedAction() {
      return exhaustedAction;
   }

   public boolean lifo() {
      return lifo;
   }

   public int maxActive() {
      return maxActive;
   }

   public int maxTotal() {
      return maxTotal;
   }

   public long maxWait() {
      return maxWait;
   }

   public int maxIdle() {
      return maxIdle;
   }

   public int minIdle() {
      return minIdle;
   }

   public int numTestsPerEvictionRun() {
      return numTestsPerEvictionRun;
   }

   public long timeBetweenEvictionRuns() {
      return timeBetweenEvictionRuns;
   }

   public long minEvictableIdleTime() {
      return minEvictableIdleTime;
   }

   public boolean testOnBorrow() {
      return testOnBorrow;
   }

   public boolean testOnReturn() {
      return testOnReturn;
   }

   public boolean testWhileIdle() {
      return testWhileIdle;
   }

   public int maxPendingRequests() {
      return maxPendingRequests;
   }

   @Override
   public String toString() {
      return "ConnectionPoolConfiguration [exhaustedAction=" + exhaustedAction + ", lifo=" + lifo + ", maxActive=" + maxActive + ", maxTotal=" + maxTotal + ", maxWait=" + maxWait
            + ", maxIdle=" + maxIdle + ", minIdle=" + minIdle + ", numTestsPerEvictionRun=" + numTestsPerEvictionRun + ", timeBetweenEvictionRuns=" + timeBetweenEvictionRuns
            + ", minEvictableIdleTime=" + minEvictableIdleTime + ", testOnBorrow=" + testOnBorrow + ", testOnReturn=" + testOnReturn + ", testWhileIdle=" + testWhileIdle
            + ", maxPendingRequests=" + maxPendingRequests + "]";
   }
}
