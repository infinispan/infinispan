package org.infinispan.client.hotrod.configuration;

/**
 * ConnectionPoolConfiguration.
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
public class ConnectionPoolConfiguration {
   private final boolean lifo;
   private final int maxTotal;
   private final int maxIdle;
   private final int minIdle;
   private final int numTestsPerEvictionRun;
   private final long timeBetweenEvictionRuns;
   private final long minEvictableIdleTime;
   private final boolean testOnBorrow;
   private final boolean testOnReturn;
   private final boolean testWhileIdle;

   ConnectionPoolConfiguration(boolean lifo, int maxTotal, int maxIdle, int minIdle, int numTestsPerEvictionRun,
         long timeBetweenEvictionRuns, long minEvictableIdleTime, boolean testOnBorrow, boolean testOnReturn, boolean testWhileIdle) {
      this.lifo = lifo;
      this.maxTotal = maxTotal;
      this.maxIdle = maxIdle;
      this.minIdle = minIdle;
      this.numTestsPerEvictionRun = numTestsPerEvictionRun;
      this.timeBetweenEvictionRuns = timeBetweenEvictionRuns;
      this.minEvictableIdleTime = minEvictableIdleTime;
      this.testOnBorrow = testOnBorrow;
      this.testOnReturn = testOnReturn;
      this.testWhileIdle = testWhileIdle;
   }

   public boolean lifo() {
      return lifo;
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

   @Override
   public String toString() {
      return "ConnectionPoolConfiguration [lifo=" + lifo + ", maxTotal=" + maxTotal
            + ", maxIdle=" + maxIdle + ", minIdle=" + minIdle + ", numTestsPerEvictionRun=" + numTestsPerEvictionRun + ", timeBetweenEvictionRuns=" + timeBetweenEvictionRuns
            + ", minEvictableIdleTime=" + minEvictableIdleTime + ", testOnBorrow=" + testOnBorrow + ", testOnReturn=" + testOnReturn + ", testWhileIdle=" + testWhileIdle + "]";
   }

}
