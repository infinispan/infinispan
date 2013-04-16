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
package org.infinispan.loaders.jdbc.mixed;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

import org.infinispan.CacheImpl;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.io.UnclosableObjectInputStream;
import org.infinispan.io.UnclosableObjectOutputStream;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheStore;
import org.infinispan.loaders.jdbc.TableManipulation;
import org.infinispan.loaders.jdbc.TableName;
import org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactoryConfig;
import org.infinispan.loaders.jdbc.stringbased.Person;
import org.infinispan.loaders.keymappers.DefaultTwoWayKey2StringMapper;
import org.infinispan.marshall.StreamingMarshaller;
import org.infinispan.marshall.TestObjectStreamMarshaller;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.infinispan.test.fwk.UnitTestDatabaseManager;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tester class for {@link JdbcMixedCacheStore}
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "loaders.jdbc.mixed.JdbcMixedCacheStoreTest")
public class JdbcMixedCacheStoreTest {

   private CacheStore cacheStore;
   private TableManipulation stringsTm;
   private TableManipulation binaryTm;
   private ConnectionFactoryConfig cfc;

   private static final Person MIRCEA = new Person("Mircea", "Markus", 28);
   private static final Person MANIK = new Person("Manik", "Surtani", 18);

   @BeforeMethod
   public void createCacheStore() throws CacheLoaderException {
      stringsTm = UnitTestDatabaseManager.buildStringTableManipulation();
      stringsTm.setTableNamePrefix("STRINGS_TABLE");
      binaryTm = UnitTestDatabaseManager.buildBinaryTableManipulation();
      binaryTm.setTableNamePrefix("BINARY_TABLE");
      cfc = UnitTestDatabaseManager.getUniqueConnectionFactoryConfig();
      JdbcMixedCacheStoreConfig cacheStoreConfig = new JdbcMixedCacheStoreConfig(cfc, binaryTm, stringsTm);
      cacheStoreConfig.setPurgeSynchronously(true);

      cacheStoreConfig.setKey2StringMapperClass(DefaultTwoWayKey2StringMapper.class.getName());
      cacheStore = new JdbcMixedCacheStore();
      cacheStore.init(cacheStoreConfig, new CacheImpl("aName"), getMarshaller());
      cacheStore.start();
   }

   @AfterMethod(alwaysRun = true)
   public void destroyStore() throws Exception {
      cacheStore.clear();
      assertBinaryRowCount(0);
      assertStringsRowCount(0);

      cacheStore.stop();
   }

   public void testMixedStore() throws Exception {
      cacheStore.store(TestInternalCacheEntryFactory.create("String", "someValue"));
      assertStringsRowCount(1);
      assertBinaryRowCount(0);
      cacheStore.store(TestInternalCacheEntryFactory.create(MIRCEA, "value"));
      assertStringsRowCount(1);
      assertStringsRowCount(1);
      assert cacheStore.load(MIRCEA).getValue().equals("value");
      assert cacheStore.load("String").getValue().equals("someValue");
   }

   public void testMultipleEntriesWithSameHashCode() throws Exception {
      Person one = new Person("Mircea", "Markus", 28);
      Person two = new Person("Manik", "Surtani", 28);
      one.setHashCode(100);
      two.setHashCode(100);
      cacheStore.store(TestInternalCacheEntryFactory.create(one, "value"));
      assertBinaryRowCount(1);
      assertStringsRowCount(0);
      cacheStore.store(TestInternalCacheEntryFactory.create(two, "otherValue"));
      assertBinaryRowCount(1); //both go to same bucket
      assertStringsRowCount(0);
      assert cacheStore.load(one).getValue().equals("value");
      assert cacheStore.load(two).getValue().equals("otherValue");
   }

   public void testClear() throws Exception {
      cacheStore.store(TestInternalCacheEntryFactory.create("String", "someValue"));
      assertRowCounts(0, 1);
      cacheStore.store(TestInternalCacheEntryFactory.create(MIRCEA, "value"));
      assertRowCounts(1, 1);
      cacheStore.clear();
      assertRowCounts(0, 0);
   }

   public void testMixedFromAndToStream() throws Exception {
      cacheStore.store(TestInternalCacheEntryFactory.create("String", "someValue"));
      cacheStore.store(TestInternalCacheEntryFactory.create("String2", "someValue"));
      cacheStore.store(TestInternalCacheEntryFactory.create(MIRCEA, "value1"));
      cacheStore.store(TestInternalCacheEntryFactory.create(MANIK, "value2"));
      assertRowCounts(2, 2);
      StreamingMarshaller marshaller = getMarshaller();
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      ObjectOutput oo = marshaller.startObjectOutput(out, false, 12);
      try {
         cacheStore.toStream(new UnclosableObjectOutputStream(oo));
      } finally {
         marshaller.finishObjectOutput(oo);
         out.close();
         cacheStore.clear();
      }
      assertRowCounts(0, 0);

      ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
      ObjectInput oi = marshaller.startObjectInput(in, false);
      try {
         cacheStore.fromStream(new UnclosableObjectInputStream(oi));
      } finally {
         marshaller.finishObjectInput(oi);
         in.close();
      }
      assertRowCounts(2, 2);
      assert cacheStore.load("String").getValue().equals("someValue");
      assert cacheStore.load("String2").getValue().equals("someValue");
      assert cacheStore.load(MIRCEA).getValue().equals("value1");
      assert cacheStore.load(MANIK).getValue().equals("value2");
   }

   public void testLoadAll() throws Exception {
      InternalCacheEntry first = TestInternalCacheEntryFactory.create("String", "someValue");
      InternalCacheEntry second = TestInternalCacheEntryFactory.create("String2", "someValue");
      InternalCacheEntry third = TestInternalCacheEntryFactory.create(MIRCEA, "value1");
      InternalCacheEntry forth = TestInternalCacheEntryFactory.create(MANIK, "value2");
      cacheStore.store(first);
      cacheStore.store(second);
      cacheStore.store(third);
      cacheStore.store(forth);
      assertRowCounts(2, 2);
      Set<InternalCacheEntry> entries = cacheStore.loadAll();
      assert entries.size() == 4 : "Expected 4 and got: " + entries;
      assert entries.contains(first);
      assert entries.contains(second);
      assert entries.contains(third);
      assert entries.contains(forth);
   }

   public void testPurgeExpired() throws Exception {
      InternalCacheEntry first = TestInternalCacheEntryFactory.create("String", "someValue", 1000);
      InternalCacheEntry second = TestInternalCacheEntryFactory.create(MIRCEA, "value1", 1000);
      cacheStore.store(first);
      cacheStore.store(second);
      assertRowCounts(1, 1);
      Thread.sleep(1200);
      cacheStore.purgeExpired();
      assertRowCounts(0, 0);
   }

   public void testPurgeExpiredWithRemainingEntries() throws Exception {
      InternalCacheEntry first = TestInternalCacheEntryFactory.create("String", "someValue", 1000);
      InternalCacheEntry second = TestInternalCacheEntryFactory.create("String2", "someValue");
      InternalCacheEntry third = TestInternalCacheEntryFactory.create(MIRCEA, "value1", 1000);
      InternalCacheEntry forth = TestInternalCacheEntryFactory.create(MANIK, "value1");
      cacheStore.store(first);
      cacheStore.store(second);
      cacheStore.store(third);
      cacheStore.store(forth);
      assertRowCounts(2, 2);
      Thread.sleep(1200);
      cacheStore.purgeExpired();
      assertRowCounts(1, 1);
   }

   private void assertRowCounts(int binary, int strings) {
      assertBinaryRowCount(binary);
      assertStringsRowCount(strings);
   }

   private void assertStringsRowCount(int rowCount) {
      JdbcMixedCacheStore store = (JdbcMixedCacheStore) cacheStore;
      ConnectionFactory connectionFactory = store.getConnectionFactory();
      TableName tableName = stringsTm.getTableName();
      int value = UnitTestDatabaseManager.rowCount(connectionFactory, tableName);
      assert value == rowCount : "Expected " + rowCount + " rows, actual value is " + value;
   }

   private void assertBinaryRowCount(int rowCount) {
      JdbcMixedCacheStore store = (JdbcMixedCacheStore) cacheStore;
      ConnectionFactory connectionFactory = store.getConnectionFactory();
      TableName tableName = binaryTm.getTableName();
      int value = UnitTestDatabaseManager.rowCount(connectionFactory, tableName);
      assert value == rowCount : "Expected " + rowCount + " rows, actual value is " + value;
   }

   protected StreamingMarshaller getMarshaller() {
      return new TestObjectStreamMarshaller(false);
   }
}
