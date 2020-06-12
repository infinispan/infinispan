package org.infinispan.query.core.tests;

import static org.testng.AssertJUnit.assertEquals;

import java.util.List;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.core.Search;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.core.tests.QueryCoreTest")
public class QueryCoreTest extends SingleCacheManagerTest {

   private static class Person {

      private String _name;

      private String _surname;

      public String getName() {
         return _name;
      }

      public void setName(String name) {
         this._name = name;
      }

      public String getSurname() {
         return _surname;
      }

      public void setSurname(String surname) {
         this._surname = surname;
      }

      @Override
      public String toString() {
         return "Person{name='" + _name + "', surname='" + _surname + "'}";
      }
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() {
      ConfigurationBuilder c = getDefaultStandaloneCacheConfig(false);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(false);
      cm.defineConfiguration("test", c.build());
      cache = cm.getCache("test");
      return cm;
   }

   public void testQuery() {
      Person spidey = new Person();
      spidey.setName("Hombre");
      spidey.setSurname("Araña");
      cache.put("key1", spidey);

      Person superMujer = new Person();
      superMujer.setName("Super");
      superMujer.setSurname("Woman");
      cache.put("key2", superMujer);

      assertEquals(2, cache.size());

      QueryFactory queryFactory = Search.getQueryFactory(cache);

      Query<Person> query = queryFactory.create("from " + Person.class.getName() + " where name='Hombre'");
      List<Person> results = query.execute().list();

      assertEquals("Araña", results.get(0).getSurname());
   }
}
