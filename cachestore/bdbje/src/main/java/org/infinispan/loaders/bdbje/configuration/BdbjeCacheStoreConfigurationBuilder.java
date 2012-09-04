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
package org.infinispan.loaders.bdbje.configuration;

import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.LoadersConfigurationBuilder;
import org.infinispan.loaders.bdbje.BdbjeCacheStore;
import org.infinispan.util.TypedProperties;

/**
 * BdbjeCacheStoreConfigurationBuilder. Configures a {@link BdbjeCacheStore}
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class BdbjeCacheStoreConfigurationBuilder extends
      AbstractStoreConfigurationBuilder<BdbjeCacheStoreConfiguration, BdbjeCacheStoreConfigurationBuilder> {
   private String location = "Infinispan-BdbjeCacheStore";
   private long lockAcquistionTimeout = 60 * 1000;
   private int maxTxRetries = 5;
   private String cacheDbNamePrefix;
   private String catalogDbName;
   private String expiryDbPrefix;
   private String environmentPropertiesFile;

   public BdbjeCacheStoreConfigurationBuilder(LoadersConfigurationBuilder builder) {
      super(builder);
   }

   @Override
   public BdbjeCacheStoreConfigurationBuilder self() {
      return this;
   }

   public BdbjeCacheStoreConfigurationBuilder location(String location) {
      this.location = location;
      return this;
   }

   public BdbjeCacheStoreConfigurationBuilder lockAcquistionTimeout(long lockAcquistionTimeout) {
      this.lockAcquistionTimeout = lockAcquistionTimeout;
      return this;
   }

   public BdbjeCacheStoreConfigurationBuilder maxTxRetries(int maxTxRetries) {
      this.maxTxRetries = maxTxRetries;
      return this;
   }

   public BdbjeCacheStoreConfigurationBuilder cacheDbNamePrefix(String cacheDbNamePrefix) {
      this.cacheDbNamePrefix = cacheDbNamePrefix;
      return this;
   }

   public BdbjeCacheStoreConfigurationBuilder catalogDbName(String catalogDbName) {
      this.catalogDbName = catalogDbName;
      return this;
   }

   public BdbjeCacheStoreConfigurationBuilder expiryDbPrefix(String expiryDbPrefix) {
      this.expiryDbPrefix = expiryDbPrefix;
      return this;
   }

   public BdbjeCacheStoreConfigurationBuilder environmentPropertiesFile(String environmentPropertiesFile) {
      this.environmentPropertiesFile = environmentPropertiesFile;
      return this;
   }

   @Override
   public BdbjeCacheStoreConfiguration create() {
      return new BdbjeCacheStoreConfiguration(location, lockAcquistionTimeout, maxTxRetries, cacheDbNamePrefix,
            catalogDbName, expiryDbPrefix, environmentPropertiesFile, purgeOnStartup, purgeSynchronously,
            purgerThreads, fetchPersistentState, ignoreModifications, TypedProperties.toTypedProperties(properties),
            async.create(), singletonStore.create());
   }

   @Override
   public BdbjeCacheStoreConfigurationBuilder read(BdbjeCacheStoreConfiguration template) {
      this.location = template.location();
      this.lockAcquistionTimeout = template.lockAcquisitionTimeout();
      this.maxTxRetries = template.maxTxRetries();
      this.cacheDbNamePrefix = template.cacheDbNamePrefix();
      this.catalogDbName = template.catalogDbName();
      this.expiryDbPrefix = template.expiryDbPrefix();
      this.environmentPropertiesFile = template.environmentPropertiesFile();
      return this;
   }

}
