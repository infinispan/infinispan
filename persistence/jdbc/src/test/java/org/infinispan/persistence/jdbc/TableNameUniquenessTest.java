package org.infinispan.persistence.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.Serializable;
import java.sql.Connection;
import java.util.Objects;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.manager.CacheContainer;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfiguration;
import org.infinispan.persistence.jdbc.impl.table.TableName;
import org.infinispan.persistence.jdbc.stringbased.JdbcStringBasedStore;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.persistence.support.WaitDelegatingNonBlockingStore;
import org.infinispan.persistence.support.WaitNonBlockingStore;
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
         cm = TestCacheManagerFactory.fromXml("configs/all/string-based.xml");
         Cache<String, String> first = cm.getCache("first");
         Cache<String, String> second = cm.getCache("second");

         StoreConfiguration firstCacheLoaderConfig = first.getCacheConfiguration().persistence().stores().get(0);
         StoreConfiguration secondCacheLoaderConfig = second.getCacheConfiguration().persistence().stores().get(0);
         assertEquals(JdbcStringBasedStoreConfiguration.class, firstCacheLoaderConfig.getClass());
         assertEquals(JdbcStringBasedStoreConfiguration.class, secondCacheLoaderConfig.getClass());

         WaitDelegatingNonBlockingStore<String, String>  firstCs = TestingUtil.getFirstStoreWait(first);
         WaitDelegatingNonBlockingStore<String, String>  secondCs = TestingUtil.getFirstStoreWait(second);


         JdbcStringBasedStore<String, String>  firstJdbcS = (JdbcStringBasedStore<String, String> ) firstCs.delegate();

         assertTableExistence(firstJdbcS.getConnectionFactory().getConnection(), firstJdbcS.getTableManager().getIdentifierQuoteString(),
                              "second", "first", "ISPN_STRING_TABLE");

         assertNoOverlapingState(first, second, firstCs, secondCs);
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
         if (!(o instanceof Person person)) return false;

         if (age != person.age) return false;
         return Objects.equals(name, person.name);
      }

      @Override
      public int hashCode() {
         int result = age;
         result = 31 * result + (name != null ? name.hashCode() : 0);
         return result;
      }
   }

   private void assertTableExistence(Connection connection, String identifierQuote, String secondTable, String firstTable, String tablePrefix) throws Exception {
      assertFalse(TableManagerTest.existsTable(connection, new TableName(identifierQuote, tablePrefix, "")), "this table should not exist!");
      assertTrue(TableManagerTest.existsTable(connection, new TableName(identifierQuote, tablePrefix, firstTable)));
      assertTrue(TableManagerTest.existsTable(connection, new TableName(identifierQuote, tablePrefix, secondTable)));
      connection.close();
   }

   private void assertNoOverlapingState(Cache<String, String> first, Cache<String, String> second,
                                        WaitNonBlockingStore<String, String> firstCs,
                                        WaitNonBlockingStore<String, String> secondCs) throws PersistenceException {
      first.put("k", "v");
      assertTrue(firstCs.contains("k"));
      assertFalse(secondCs.contains("k"));
      assertEquals("v", first.get("k"));
      assertNull(second.get("k"));

      second.put("k2", "v2");
      assertEquals("v2", second.get("k2"));
      assertNull(first.get("k2"));
   }
}
