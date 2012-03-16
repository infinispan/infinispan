/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.config.parsing;

import org.infinispan.config.CacheLoaderManagerConfig;
import org.infinispan.config.Configuration;
import org.infinispan.config.InfinispanConfiguration;
import org.infinispan.loaders.CacheStoreConfig;
import org.infinispan.loaders.decorators.SingletonStoreConfig;
import org.infinispan.loaders.jdbc.TableManipulation;
import org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactoryConfig;
import org.infinispan.loaders.jdbc.connectionfactory.PooledConnectionFactory;
import org.infinispan.loaders.jdbc.stringbased.JdbcStringBasedCacheStore;
import org.infinispan.loaders.jdbc.stringbased.JdbcStringBasedCacheStoreConfig;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Map;

@Test(groups = "unit", testName = "config.parsing.JdbcConfigurationTest")
public class JdbcConfigurationTest {
   
   public void testParseCacheLoaders() throws Exception {
      InfinispanConfiguration configuration = InfinispanConfiguration.newInfinispanConfiguration("configs/jdbc-parsing-test.xml", Thread.currentThread().getContextClassLoader());
      Map<String, Configuration> namedConfigurations = configuration.parseNamedConfigurations();
      Configuration c = namedConfigurations.get("withJDBCLoader");
      CacheLoaderManagerConfig clc = c.getCacheLoaderManagerConfig();
      assert clc != null;
      assert clc.isFetchPersistentState();
      assert clc.isPassivation();
      assert clc.isShared();
      assert clc.isPreload();

      CacheStoreConfig iclc = (CacheStoreConfig) clc.getFirstCacheLoaderConfig();
      assert iclc.getCacheLoaderClassName().equals(JdbcStringBasedCacheStore.class.getName());
      assert iclc.getAsyncStoreConfig().isEnabled();
      assert iclc.getAsyncStoreConfig().getFlushLockTimeout() == 10000;
      assert iclc.getAsyncStoreConfig().getThreadPoolSize() == 10;
      assert iclc.isFetchPersistentState();
      assert iclc.isIgnoreModifications();
      assert iclc.isPurgeOnStartup();

      assert clc.getCacheLoaderConfigs().size() == 1;
      JdbcStringBasedCacheStoreConfig csConf = (JdbcStringBasedCacheStoreConfig) clc.getFirstCacheLoaderConfig();
      assert csConf.getCacheLoaderClassName().equals("org.infinispan.loaders.jdbc.stringbased.JdbcStringBasedCacheStore");
      assert csConf.isFetchPersistentState();
      assert csConf.isIgnoreModifications();
      assert csConf.isPurgeOnStartup();
      TableManipulation tableManipulation = csConf.getTableManipulation();
      ConnectionFactoryConfig cfc = csConf.getConnectionFactoryConfig();
      assert cfc.getConnectionFactoryClass().equals(PooledConnectionFactory.class.getName());
      assert cfc.getConnectionUrl().equals("jdbc://some-url");
      assert cfc.getUserName().equals("root");
      assert cfc.getDriverClass().equals("org.dbms.Driver");
      assert tableManipulation.getIdColumnType().equals("VARCHAR2(256)");
      assert tableManipulation.getDataColumnType().equals("BLOB");
      assert tableManipulation.isDropTableOnExit();
      assert !tableManipulation.isCreateTableOnStart();


      SingletonStoreConfig ssc = iclc.getSingletonStoreConfig();
      assert ssc.isSingletonStoreEnabled();
      assert ssc.isPushStateWhenCoordinator();
      assert ssc.getPushStateTimeout() == 20000;
   }

   @Test(expectedExceptions={org.infinispan.CacheException.class})
   public void testWrongStoreConfiguration() throws IOException {
      CacheContainer cm = null;
      try {
         cm = new DefaultCacheManager("configs/illegal.xml");
         cm.start();
         //needs to get at least a cache to reproduce:
         cm.getCache("AnyCache");
      } finally {
         TestingUtil.killCacheManagers(cm);
      }
   }
}
