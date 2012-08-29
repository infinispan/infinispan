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

import org.infinispan.configuration.Builder;

public class ConnectionPoolConfigurationBuilder extends AbstractRemoteCacheStoreConfigurationChildBuilder implements
      Builder<ConnectionPoolConfiguration> {
   private ExhaustedAction exhaustedAction = ExhaustedAction.WAIT;
   private int maxActive = -1;
   private int maxTotal = -1;
   private int maxIdle = -1;
   private int minIdle = 1;
   private long timeBetweenEvictionRuns = 120000;
   private long minEvictableIdleTime = 1800000;
   private boolean testWhileIdle = true;

   ConnectionPoolConfigurationBuilder(RemoteCacheStoreConfigurationBuilder builder) {
      super(builder);
   }

   public ConnectionPoolConfigurationBuilder exhaustedAction(ExhaustedAction exhaustedAction) {
      this.exhaustedAction = exhaustedAction;
      return this;
   }

   public ConnectionPoolConfigurationBuilder maxActive(int maxActive) {
      this.maxActive = maxActive;
      return this;
   }

   public ConnectionPoolConfigurationBuilder maxTotal(int maxTotal) {
      this.maxTotal = maxTotal;
      return this;
   }

   public ConnectionPoolConfigurationBuilder maxIdle(int maxIdle) {
      this.maxIdle = maxIdle;
      return this;
   }

   public ConnectionPoolConfigurationBuilder minIdle(int minIdle) {
      this.minIdle = minIdle;
      return this;
   }

   public ConnectionPoolConfigurationBuilder timeBetweenEvictionRuns(long timeBetweenEvictionRuns) {
      this.timeBetweenEvictionRuns = timeBetweenEvictionRuns;
      return this;
   }

   public ConnectionPoolConfigurationBuilder minEvictableIdleTime(long minEvictableIdleTime) {
      this.minEvictableIdleTime = minEvictableIdleTime;
      return this;
   }

   public ConnectionPoolConfigurationBuilder testWhileIdle(boolean testWhileIdle) {
      this.testWhileIdle = testWhileIdle;
      return this;
   }

   @Override
   public void validate() {
   }

   @Override
   public ConnectionPoolConfiguration create() {
      return new ConnectionPoolConfiguration(exhaustedAction, maxActive, maxTotal, maxIdle, minIdle, timeBetweenEvictionRuns,
            minEvictableIdleTime, testWhileIdle);
   }

   @Override
   public ConnectionPoolConfigurationBuilder read(ConnectionPoolConfiguration template) {
      exhaustedAction = template.exhaustedAction();
      maxActive = template.maxActive();
      maxTotal = template.maxTotal();
      maxIdle = template.maxIdle();
      minIdle = template.minIdle();
      timeBetweenEvictionRuns = template.timeBetweenEvictionRuns();
      minEvictableIdleTime = template.minEvictableIdleTime();
      testWhileIdle = template.testWhileIdle();
      return this;
   }

}
