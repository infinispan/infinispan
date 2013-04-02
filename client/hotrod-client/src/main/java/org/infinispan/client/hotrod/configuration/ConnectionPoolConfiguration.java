/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
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

   ConnectionPoolConfiguration(ExhaustedAction exhaustedAction, boolean lifo, int maxActive, int maxTotal, long maxWait, int maxIdle, int minIdle, int numTestsPerEvictionRun,
         long timeBetweenEvictionRuns, long minEvictableIdleTime, boolean testOnBorrow, boolean testOnReturn, boolean testWhileIdle) {
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

   @Override
   public String toString() {
      return "ConnectionPoolConfiguration [exhaustedAction=" + exhaustedAction + ", lifo=" + lifo + ", maxActive=" + maxActive + ", maxTotal=" + maxTotal + ", maxWait=" + maxWait
            + ", maxIdle=" + maxIdle + ", minIdle=" + minIdle + ", numTestsPerEvictionRun=" + numTestsPerEvictionRun + ", timeBetweenEvictionRuns=" + timeBetweenEvictionRuns
            + ", minEvictableIdleTime=" + minEvictableIdleTime + ", testOnBorrow=" + testOnBorrow + ", testOnReturn=" + testOnReturn + ", testWhileIdle=" + testWhileIdle + "]";
   }

}
