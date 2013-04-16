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
package org.infinispan.loaders.jdbc;

import org.infinispan.Cache;
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.CacheStore;
import org.infinispan.loaders.jdbc.binary.JdbcBinaryCacheStore;
import org.infinispan.loaders.jdbc.mixed.JdbcMixedCacheStore;
import org.infinispan.loaders.jdbc.stringbased.JdbcStringBasedCacheStore;
import org.infinispan.loaders.jdbc.stringbased.JdbcStringBasedCacheStoreConfig;
import org.infinispan.manager.CacheContainer;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.io.Serializable;
import java.sql.Connection;

/**
 * Test to make sure that no two caches will use the same table for storing data.
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "loaders.jdbc.TableNameUniquenessTest")
public class TableNameUniquenessTest extends AbstractInfinispanTest {

   public void testForJdbcStringBasedCacheStore() throws Exception {
      CacheContainer cm = null;
      try {
         cm = TestCacheManagerFactory.fromXml("configs/string-based.xml");
         Cache<String, String> first = cm.getCache("first");
         Cache<String, String> second = cm.getCache("second");

         CacheLoaderConfig firstCacheLoaderConfig = first.getConfiguration().getCacheLoaderManagerConfig().getFirstCacheLoaderConfig();
         assert firstCacheLoaderConfig != null;
         CacheLoaderConfig secondCacheLoaderConfig = second.getConfiguration().getCacheLoaderManagerConfig().getFirstCacheLoaderConfig();
         assert secondCacheLoaderConfig != null;
         assert firstCacheLoaderConfig instanceof JdbcStringBasedCacheStoreConfig;
         assert secondCacheLoaderConfig instanceof JdbcStringBasedCacheStoreConfig;

         JdbcStringBasedCacheStore firstCs = (JdbcStringBasedCacheStore) TestingUtil.extractComponent(first, CacheLoaderManager.class).getCacheLoader();
         JdbcStringBasedCacheStore secondCs = (JdbcStringBasedCacheStore) TestingUtil.extractComponent(second, CacheLoaderManager.class).getCacheLoader();

         assertTableExistence(firstCs.getConnectionFactory().getConnection(), firstCs.getTableManipulation().getIdentifierQuoteString(), "second", "first", "ISPN_STRING_TABLE");

         assertNoOverlapingState(first, second, firstCs, secondCs);
      } finally {
         TestingUtil.killCacheManagers(cm);
      }
   }

   public void testForJdbcBinaryCacheStore() throws Exception {
      CacheContainer cm = null;
      try {
         cm = TestCacheManagerFactory.fromXml("configs/binary.xml");
         Cache<String, String> first = cm.getCache("first");
         Cache<String, String> second = cm.getCache("second");

         JdbcBinaryCacheStore firstCs = (JdbcBinaryCacheStore) TestingUtil.extractComponent(first, CacheLoaderManager.class).getCacheLoader();
         JdbcBinaryCacheStore secondCs = (JdbcBinaryCacheStore) TestingUtil.extractComponent(second, CacheLoaderManager.class).getCacheLoader();

         assertTableExistence(firstCs.getConnectionFactory().getConnection(), firstCs.getTableManipulation().getIdentifierQuoteString(), "second", "first", "ISPN_BUCKET_TABLE");

         assertNoOverlapingState(first, second, firstCs, secondCs);
      } finally {
         TestingUtil.killCacheManagers(cm);
      }

   }

   @SuppressWarnings("unchecked")
   public void testForMixedCacheStore() throws Exception {
      CacheContainer cm = null;
      try {
         cm = TestCacheManagerFactory.fromXml("configs/mixed.xml");
         Cache first = cm.getCache("first");
         Cache second = cm.getCache("second");

         JdbcMixedCacheStore firstCs = (JdbcMixedCacheStore) TestingUtil.extractComponent(first, CacheLoaderManager.class).getCacheLoader();
         JdbcMixedCacheStore secondCs = (JdbcMixedCacheStore) TestingUtil.extractComponent(second, CacheLoaderManager.class).getCacheLoader();

         assertTableExistence(firstCs.getConnectionFactory().getConnection(), firstCs.getBinaryCacheStore().getTableManipulation().getIdentifierQuoteString(), "second", "first", "ISPN_MIXED_STR_TABLE");
         assertTableExistence(firstCs.getConnectionFactory().getConnection(), firstCs.getBinaryCacheStore().getTableManipulation().getIdentifierQuoteString(), "second", "first", "ISPN_MIXED_BINARY_TABLE");

         assertNoOverlapingState(first, second, firstCs, secondCs);


         Person person1 = new Person(29, "Mircea");
         Person person2 = new Person(29, "Manik");

         first.put("k", person1);
         assert firstCs.containsKey("k");
         assert !secondCs.containsKey("k");
         assert first.get("k").equals(person1);
         assert second.get("k") == null;

         second.put("k2", person2);
         assert second.get("k2").equals(person2);
         assert first.get("k2") == null;
      } finally {
         TestingUtil.killCacheManagers(cm);
      }

   }


   static class Person implements Serializable {
      int age;
      String name;
      private static final long serialVersionUID = 4227565864228124235L;

      Person(int age, String name) {
         this.age = age;
         this.name = name;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (!(o instanceof Person)) return false;

         Person person = (Person) o;

         if (age != person.age) return false;
         if (name != null ? !name.equals(person.name) : person.name != null) return false;

         return true;
      }

      @Override
      public int hashCode() {
         int result = age;
         result = 31 * result + (name != null ? name.hashCode() : 0);
         return result;
      }
   }

   private void assertTableExistence(Connection connection, String identifierQuote, String secondTable, String firstTable, String tablePrefix) throws Exception {
      assert !TableManipulationTest.existsTable(connection, new TableName(identifierQuote, tablePrefix, "")) : "this table should not exist!";
      assert TableManipulationTest.existsTable(connection, new TableName(identifierQuote, tablePrefix, firstTable));
      assert TableManipulationTest.existsTable(connection, new TableName(identifierQuote, tablePrefix, secondTable));
      connection.close();
   }

   private String quoteTableName(String quote, String tableName) {
      return quote + tableName + quote;
   }

   private void assertNoOverlapingState(Cache<String, String> first, Cache<String, String> second, CacheStore firstCs, CacheStore secondCs) throws CacheLoaderException {
      first.put("k", "v");
      assert firstCs.containsKey("k");
      assert !secondCs.containsKey("k");
      assert first.get("k").equals("v");
      assert second.get("k") == null;

      second.put("k2", "v2");
      assert second.get("k2").equals("v2");
      assert first.get("k2") == null;
   }
}
