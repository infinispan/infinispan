package org.infinispan.persistence.jdbc;

import static org.testng.AssertJUnit.assertEquals;

import java.io.Serializable;
import java.sql.Connection;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.manager.CacheContainer;
import org.infinispan.marshall.core.ExternalPojo;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfiguration;
import org.infinispan.persistence.jdbc.stringbased.JdbcStringBasedStore;
import org.infinispan.persistence.jdbc.impl.table.TableName;
import org.infinispan.persistence.spi.CacheLoader;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Test to make sure that no two caches will use the same table for storing data.
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "persistence.jdbc.TableNameUniquenessTest")
public class TableNameUniquenessTest extends AbstractInfinispanTest {

   public void testForJdbcStringBasedCacheStore() throws Exception {
      CacheContainer cm = null;
      try {
         cm = TestCacheManagerFactory.fromXml("configs/string-based.xml");
         Cache<String, String> first = cm.getCache("first");
         Cache<String, String> second = cm.getCache("second");

         StoreConfiguration firstCacheLoaderConfig = first.getCacheConfiguration().persistence().stores().get(0);
         StoreConfiguration secondCacheLoaderConfig = second.getCacheConfiguration().persistence().stores().get(0);
         assertEquals(JdbcStringBasedStoreConfiguration.class, firstCacheLoaderConfig.getClass());
         assertEquals(JdbcStringBasedStoreConfiguration.class, secondCacheLoaderConfig.getClass());

         JdbcStringBasedStore firstCs = (JdbcStringBasedStore) TestingUtil.getFirstLoader(first);
         JdbcStringBasedStore secondCs = (JdbcStringBasedStore) TestingUtil.getFirstLoader(second);

         assertTableExistence(firstCs.getConnectionFactory().getConnection(), firstCs.getTableManager().getIdentifierQuoteString(),
                              "second", "first", "ISPN_STRING_TABLE");

         assertNoOverlapingState(first, second, firstCs, secondCs);
      } finally {
         TestingUtil.killCacheManagers(cm);
      }
   }

   static class Person implements Serializable, ExternalPojo {
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
      assert !TableManagerTest.existsTable(connection, new TableName(identifierQuote, tablePrefix, "")) : "this table should not exist!";
      assert TableManagerTest.existsTable(connection, new TableName(identifierQuote, tablePrefix, firstTable));
      assert TableManagerTest.existsTable(connection, new TableName(identifierQuote, tablePrefix, secondTable));
      connection.close();
   }

   private void assertNoOverlapingState(Cache first, Cache second, CacheLoader firstCs, CacheLoader secondCs) throws PersistenceException {
      first.put("k", "v");
      assert firstCs.contains("k");
      assert !secondCs.contains("k");
      assert first.get("k").equals("v");
      assert second.get("k") == null;

      second.put("k2", "v2");
      assert second.get("k2").equals("v2");
      assert first.get("k2") == null;
   }
}
