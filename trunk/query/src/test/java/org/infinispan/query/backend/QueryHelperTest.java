package org.infinispan.query.backend;

import org.infinispan.Cache;
import org.infinispan.CacheException;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.query.test.Person;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Test class for the {@link org.infinispan.query.backend.QueryHelper}
 *
 * @author Navin Surtani
 * @since 4.0
 */

@Test (groups = "functional")
public class QueryHelperTest {

   @BeforeMethod
   public void setUp(){
      System.setProperty("infinispan.query.enabled", "true");
      System.setProperty("infinispan.query.indexLocalOnly", "true");
   }

   @Test (expectedExceptions = IllegalArgumentException.class)
   public void testConstructorWithNoClasses(){
      Cache c = new DefaultCacheManager().getCache();
      Class[] classes = new Class[0];
      QueryHelper qh = new QueryHelper(c, null, classes);
   }

   @Test (expectedExceptions = CacheException.class)
   public void  testCheckInterceptorChainWithIndexLocalTrue(){
      Cache c = new DefaultCacheManager().getCache();
      QueryHelper qh = new QueryHelper(c, null, Person.class);
      QueryHelper qh2 = new QueryHelper(c, null, Person.class);

   }

   @Test (expectedExceptions = CacheException.class)
   public void  testCheckInterceptorChainWithIndexLocalFalse(){
      System.setProperty("infinispan.query.indexLocalOnly", "false");

      Cache c = new DefaultCacheManager().getCache();
      QueryHelper qh = new QueryHelper(c, null, Person.class);
      QueryHelper qh2 = new QueryHelper(c, null, Person.class);
   }

   public void testTwoQueryHelpersWithTwoCaches(){

      Cache c1 = new DefaultCacheManager().getCache();
      Cache c2 = new DefaultCacheManager().getCache();

      QueryHelper qh1 = new QueryHelper(c1, null, Person.class);
      QueryHelper qh2 = new QueryHelper(c2, null, Person.class);
   }
}
