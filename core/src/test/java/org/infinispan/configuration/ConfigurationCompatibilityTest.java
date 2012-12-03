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
package org.infinispan.configuration;

import org.infinispan.configuration.cache.CacheStoreConfigurationBuilder;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.FileCacheStoreConfigurationBuilder;
import org.infinispan.configuration.cache.LoaderConfigurationBuilder;
import org.infinispan.configuration.cache.LoadersConfigurationBuilder;
import org.infinispan.loaders.file.FileCacheStore;
import org.testng.annotations.Test;

@Test(groups = "functional")
public class ConfigurationCompatibilityTest {

   public void testModeShapeStoreConfiguration() {
      // This code courtesy of Randall Hauch
      ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
      LoaderConfigurationBuilder lb = configurationBuilder.loaders().addCacheLoader().cacheLoader(new FileCacheStore());
      lb.addProperty("dropTableOnExit", "false").addProperty("createTableOnStart", "true")
            .addProperty("connectionFactoryClass", "org.infinispan.loaders.jdbc.connectionfactory.PooledConnectionFactory")
            .addProperty("connectionUrl", "jdbc:h2:file:/abs/path/string_based_db;DB_CLOSE_DELAY=1").addProperty("driverClass", "org.h2.Driver").addProperty("userName", "sa")
            .addProperty("idColumnName", "ID_COLUMN").addProperty("idColumnType", "VARCHAR(255)").addProperty("timestampColumnName", "TIMESTAMP_COLUMN")
            .addProperty("timestampColumnType", "BIGINT").addProperty("dataColumnName", "DATA_COLUMN").addProperty("dataColumnType", "BINARY")
            .addProperty("bucketTableNamePrefix", "MODE").addProperty("cacheName", "default");
   }

   public void testAS71StoreConfiguration() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.loaders().shared(false).preload(true).passivation(false);
      LoaderConfigurationBuilder storeBuilder = builder.loaders().addCacheLoader().fetchPersistentState(false).purgeOnStartup(false).purgeSynchronously(true);
      storeBuilder.singletonStore().enabled(false);
   }

   public void testAS72StoreConfiguration() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      LoadersConfigurationBuilder loadersBuilder = builder.loaders().shared(false).preload(true).passivation(false);
      CacheStoreConfigurationBuilder<?, ?> storeBuilder = loadersBuilder.addStore(FileCacheStoreConfigurationBuilder.class).location("/tmp").fetchPersistentState(false)
            .purgeOnStartup(false).purgeSynchronously(true);
      storeBuilder.singletonStore().enabled(false);

   }

   public void testDocumentationCacheLoadersConfiguration() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.loaders()
         .passivation(false).shared(false).preload(true)
         .addFileCacheStore()
            .fetchPersistentState(true)
            .purgerThreads(3)
            .purgeSynchronously(true)
            .ignoreModifications(false)
            .purgeOnStartup(false)
            .location(System.getProperty("java.io.tmpdir"))
            .async()
               .enabled(true)
               .flushLockTimeout(15000)
               .threadPoolSize(5)
            .singletonStore()
              .enabled(true)
              .pushStateWhenCoordinator(true)
              .pushStateTimeout(20000);
   }

}
