package org.infinispan.query.timeout;

import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.commons.api.query.Query;
import org.infinispan.query.test.Person;

class TestHelper {

   static void runFullTextQueryWithTimeout(Cache<?, ?> cache, long timeout, TimeUnit timeUnit) {
      String q = String.format("FROM %s WHERE name:'name_*'", Person.class.getName());
      Query<?> query = cache.query(q).timeout(timeout, timeUnit);
      query.execute();
   }

   static void runRegularQueryWithTimeout(Cache<?, ?> cache, long timeout, TimeUnit timeUnit) {
      String q = String.format("FROM %s WHERE name LIKE 'name_%%'", Person.class.getName());
      Query<?> query = cache.query(q).timeout(timeout, timeUnit);
      query.execute();
   }

   static void runRegularSortedQueryWithTimeout(Cache<?, ?> cache, long timeout, TimeUnit timeUnit) {
      String q = String.format("FROM %s WHERE name LIKE 'name_%%' ORDER BY name", Person.class.getName());
      Query<?> query = cache.query(q).timeout(timeout, timeUnit);
      query.execute();
   }

   static void populate(Cache cache, int numEntries) {
      for (int i = 0; i < numEntries; i++) {
         cache.put(i, new Person("name_" + i, "", 0));
      }
   }
}
