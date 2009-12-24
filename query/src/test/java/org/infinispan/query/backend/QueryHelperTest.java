package org.infinispan.query.backend;

import org.infinispan.Cache;
import org.infinispan.CacheException;
import org.infinispan.config.Configuration;
import org.infinispan.manager.CacheManager;
import org.infinispan.query.test.Person;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.LinkedList;
import java.util.List;

/**
 * Test class for the {@link org.infinispan.query.backend.QueryHelper}
 *
 * @author Navin Surtani
 * @since 4.0
 */

@Test(groups = "unit")
public class QueryHelperTest {
   Configuration cfg;
   List<CacheManager> cacheManagers;

   @BeforeMethod
   public void setUp() {
      cfg = new Configuration();
      cfg.setIndexingEnabled(true);
      cfg.setIndexLocalOnly(true);

      cacheManagers = new LinkedList<CacheManager>();
   }

   @AfterMethod
   public void tearDown() {
      TestingUtil.killCacheManagers(cacheManagers);
   }

   private Cache<?, ?> createCache(Configuration c) {
      CacheManager cm = TestCacheManagerFactory.createCacheManager(c);
      cacheManagers.add(cm);
      return cm.getCache();
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testConstructorWithNoClasses() {
      Cache<?, ?> c = createCache(cfg);
      Class[] classes = new Class[0];
      QueryHelper qh = new QueryHelper(c, null, classes);
   }

   @Test(expectedExceptions = CacheException.class)
   public void testCheckInterceptorChainWithIndexLocalTrue() {
      Cache<?, ?> c = createCache(cfg);
      QueryHelper qh = new QueryHelper(c, null, Person.class);
      QueryHelper qh2 = new QueryHelper(c, null, Person.class);
   }

   @Test(expectedExceptions = CacheException.class)
   public void testCheckInterceptorChainWithIndexLocalFalse() {
      cfg.setIndexLocalOnly(false);
      Cache<?, ?> c = createCache(cfg);
      QueryHelper qh = new QueryHelper(c, null, Person.class);
      QueryHelper qh2 = new QueryHelper(c, null, Person.class);
   }

   public void testTwoQueryHelpersWithTwoCaches() {
      Cache c1 = createCache(cfg);
      Cache c2 = createCache(cfg);

      QueryHelper qh1 = new QueryHelper(c1, null, Person.class);
      QueryHelper qh2 = new QueryHelper(c2, null, Person.class);
   }
}
