/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.loaders.jdbc.stringbased;

import org.infinispan.Cache;
import org.infinispan.CacheImpl;
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.CacheStore;
import org.infinispan.loaders.jdbc.ManagedConnectionFactoryTest;
import org.infinispan.loaders.jdbc.TableManipulation;
import org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactoryConfig;
import org.infinispan.loaders.jdbc.connectionfactory.ManagedConnectionFactory;
import org.infinispan.loaders.keymappers.UnsupportedKeyTypeException;
import org.infinispan.manager.CacheContainer;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.UnitTestDatabaseManager;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 */
@Test (groups = "functional", testName = "loaders.jdbc.stringbased.StringStoreWithManagedConnectionTest")
public class StringStoreWithManagedConnectionTest extends ManagedConnectionFactoryTest {

   protected CacheStore createCacheStore() throws Exception {
      ConnectionFactoryConfig connectionFactoryConfig = new ConnectionFactoryConfig();
      connectionFactoryConfig.setConnectionFactoryClass(ManagedConnectionFactory.class.getName());
      connectionFactoryConfig.setDatasourceJndiLocation(getDatasourceLocation());
      TableManipulation tm = UnitTestDatabaseManager.buildDefaultTableManipulation();
      JdbcStringBasedCacheStoreConfig config = new JdbcStringBasedCacheStoreConfig(connectionFactoryConfig, tm);
      JdbcStringBasedCacheStore stringBasedCacheStore = new JdbcStringBasedCacheStore();
      stringBasedCacheStore.init(config, new CacheImpl("aName"), getMarshaller());
      stringBasedCacheStore.start();
      return stringBasedCacheStore;
   }

   public void testLoadFromFile() throws Exception {
      CacheContainer cm = null;
      try {
         cm = TestCacheManagerFactory.fromXml("configs/managed/str-managed-connection-factory.xml");
         Cache<String, String> first = cm.getCache("first");
         Cache<String, String> second = cm.getCache("second");

         CacheLoaderConfig firstCacheLoaderConfig = first.getConfiguration().getCacheLoaderManagerConfig().getFirstCacheLoaderConfig();
         assert firstCacheLoaderConfig != null;
         CacheLoaderConfig secondCacheLoaderConfig = second.getConfiguration().getCacheLoaderManagerConfig().getFirstCacheLoaderConfig();
         assert secondCacheLoaderConfig != null;
         assert firstCacheLoaderConfig instanceof JdbcStringBasedCacheStoreConfig;
         assert secondCacheLoaderConfig instanceof JdbcStringBasedCacheStoreConfig;
         CacheLoaderManager cacheLoaderManager = first.getAdvancedCache().getComponentRegistry().getComponent(CacheLoaderManager.class);
         JdbcStringBasedCacheStore loader = (JdbcStringBasedCacheStore) cacheLoaderManager.getCacheLoader();
         assert loader.getConnectionFactory() instanceof ManagedConnectionFactory;
      } finally {
         TestingUtil.killCacheManagers(cm);
      }
   }

   @Override
   public String getDatasourceLocation() {
      return "java:/StringStoreWithManagedConnectionTest/DS";
   }

   @Override
   @Test(expectedExceptions = UnsupportedKeyTypeException.class)
   public void testLoadAndStoreMarshalledValues() throws CacheLoaderException {
      super.testLoadAndStoreMarshalledValues();
   }

}
