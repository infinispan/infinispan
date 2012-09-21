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
package org.infinispan.loaders.remote.configuration;

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
