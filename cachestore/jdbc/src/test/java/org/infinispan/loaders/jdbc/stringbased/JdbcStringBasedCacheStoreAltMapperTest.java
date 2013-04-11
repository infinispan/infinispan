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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.infinispan.Cache;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheStore;
import org.infinispan.loaders.jdbc.TableManipulation;
import org.infinispan.loaders.jdbc.TableName;
import org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactoryConfig;
import org.infinispan.loaders.keymappers.UnsupportedKeyTypeException;
import org.infinispan.marshall.StreamingMarshaller;
import org.infinispan.marshall.TestObjectStreamMarshaller;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.infinispan.test.fwk.UnitTestDatabaseManager;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 * Tester for {@link JdbcStringBasedCacheStore} with an alternative {@link org.infinispan.loaders.keymappers.Key2StringMapper}.
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "loaders.jdbc.stringbased.JdbcStringBasedCacheStoreAltMapperTest")
public class JdbcStringBasedCacheStoreAltMapperTest {

   CacheStore cacheStore;
   private ConnectionFactoryConfig cfc;
   private TableManipulation tableManipulation;
   private static final Person MIRCEA = new Person("Mircea", "Markus", 28);
   private static final Person MANIK = new Person("Manik", "Surtani", 18);

   @BeforeTest
   public void createCacheStore() throws CacheLoaderException {
      tableManipulation = UnitTestDatabaseManager.buildStringTableManipulation();
      cfc = UnitTestDatabaseManager.getUniqueConnectionFactoryConfig();
      JdbcStringBasedCacheStoreConfig config = new JdbcStringBasedCacheStoreConfig(cfc, tableManipulation);
      config.setKey2StringMapperClass(PersonKey2StringMapper.class.getName());
      config.setPurgeSynchronously(true);
      cacheStore = new JdbcStringBasedCacheStore();
      Cache<?, ?> mockCache = mock(Cache.class);
      when(mockCache.getName()).thenReturn(getClass().getName());
      cacheStore.init(config, mockCache, getMarshaller());
      cacheStore.start();
   }

   @AfterMethod(alwaysRun = true)
   public void clearStore() throws Exception {
      cacheStore.clear();
      assert rowCount() == 0;
   }

   @AfterTest(alwaysRun = true)
   public void destroyStore() throws CacheLoaderException {
      cacheStore.stop();
   }

   /**
    * When trying to persist an unsupported object an exception is expected.
    */
   public void persistUnsupportedObject() throws Exception {
      try {
         cacheStore.store(TestInternalCacheEntryFactory.create("key", "value"));
         assert false : "exception is expected as PersonKey2StringMapper does not support strings";
      } catch (UnsupportedKeyTypeException e) {
         assert true : "expected";
      }
      //just check that an person object will be persisted okay
      cacheStore.store(TestInternalCacheEntryFactory.create(MIRCEA, "Cluj Napoca"));
   }


   public void testStoreLoadRemove() throws Exception {
      assert rowCount() == 0;
      assert cacheStore.load(MIRCEA) == null : "should not be present in the store";
      String value = "adsdsadsa";
      cacheStore.store(TestInternalCacheEntryFactory.create(MIRCEA, value));
      assert rowCount() == 1;
      assert cacheStore.load(MIRCEA).getValue().equals(value);
      assert !cacheStore.remove(MANIK);
      assert cacheStore.load(MIRCEA).getValue().equals(value);
      assert rowCount() == 1;
      assert cacheStore.remove(MIRCEA);
      assert rowCount() == 0;
   }

   public void testRemoveAll() throws Exception {
      assert rowCount() == 0;
      cacheStore.store(TestInternalCacheEntryFactory.create(MIRCEA, "value"));
      cacheStore.store(TestInternalCacheEntryFactory.create(MANIK, "value"));
      assert rowCount() == 2;
      cacheStore.removeAll(Collections.singleton((Object) MIRCEA));
      assert cacheStore.load(MANIK).getValue().equals("value");
      assert rowCount() == 1;
      cacheStore.store(TestInternalCacheEntryFactory.create(MIRCEA, "value"));
      assert rowCount() == 2;
      Set<Object> toRemove = new HashSet<Object>();
      toRemove.add(MIRCEA);
      toRemove.add(MANIK);
      cacheStore.removeAll(toRemove);
      assert rowCount() == 0;
   }

   public void testClear() throws Exception {
      assert rowCount() == 0;
      cacheStore.store(TestInternalCacheEntryFactory.create(MIRCEA, "value"));
      cacheStore.store(TestInternalCacheEntryFactory.create(MANIK, "value"));
      assert rowCount() == 2;
      cacheStore.clear();
      assert rowCount() == 0;
   }

   public void testPurgeExpired() throws Exception {
      InternalCacheEntry first = TestInternalCacheEntryFactory.create(MIRCEA, "val", 1000);
      InternalCacheEntry second = TestInternalCacheEntryFactory.create(MANIK, "val2");
      cacheStore.store(first);
      cacheStore.store(second);
      assert rowCount() == 2;
      Thread.sleep(1100);
//      printTableContent();
      cacheStore.purgeExpired();
      assert rowCount() == 1;
      assert cacheStore.load(MANIK).getValue().equals("val2");
   }

   private int rowCount() {
      ConnectionFactory connectionFactory = getConnection();
      TableName tableName = tableManipulation.getTableName();
      return UnitTestDatabaseManager.rowCount(connectionFactory, tableName);
   }

   private ConnectionFactory getConnection() {

      JdbcStringBasedCacheStore store = (JdbcStringBasedCacheStore) cacheStore;
      return store.getConnectionFactory();
   }

   protected StreamingMarshaller getMarshaller() {
      return new TestObjectStreamMarshaller(false);
   }
}
