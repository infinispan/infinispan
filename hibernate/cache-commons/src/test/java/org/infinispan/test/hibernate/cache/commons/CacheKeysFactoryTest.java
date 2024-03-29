package org.infinispan.test.hibernate.cache.commons;

import static org.infinispan.test.hibernate.cache.commons.util.TxUtil.withTxSession;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;

import org.hibernate.SessionFactory;
import org.hibernate.cache.internal.DefaultCacheKeysFactory;
import org.hibernate.cache.internal.SimpleCacheKeysFactory;
import org.hibernate.cache.spi.CacheImplementor;
import org.hibernate.cache.spi.RegionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;
import org.hibernate.testing.junit4.BaseUnitTestCase;
import org.infinispan.Cache;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.test.hibernate.cache.commons.functional.entities.Name;
import org.infinispan.test.hibernate.cache.commons.functional.entities.Person;
import org.infinispan.test.hibernate.cache.commons.util.InfinispanTestingSetup;
import org.infinispan.test.hibernate.cache.commons.util.TestRegionFactory;
import org.infinispan.test.hibernate.cache.commons.util.TestRegionFactoryProvider;
import org.junit.Rule;
import org.junit.Test;

public class CacheKeysFactoryTest extends BaseUnitTestCase {
   @Rule
   public InfinispanTestingSetup infinispanTestIdentifier = new InfinispanTestingSetup();

   private SessionFactory getSessionFactory(String cacheKeysFactory) {
      Configuration configuration = new Configuration()
         .setProperty(Environment.USE_SECOND_LEVEL_CACHE, "true")
         .setProperty(Environment.CACHE_REGION_FACTORY, TestRegionFactoryProvider.load().getRegionFactoryClass().getName())
         .setProperty(Environment.DEFAULT_CACHE_CONCURRENCY_STRATEGY, "transactional")
         .setProperty("javax.persistence.sharedCache.mode", "ALL")
         .setProperty(Environment.HBM2DDL_AUTO, "create-drop");
      if (cacheKeysFactory != null) {
         configuration.setProperty(Environment.CACHE_KEYS_FACTORY, cacheKeysFactory);
      }
      configuration.addAnnotatedClass(Person.class);
      return configuration.buildSessionFactory();
   }

   @Test
   public void testNotSet() throws Exception {
      test(null, "CacheKeyImplementation");
   }

   @Test
   public void testDefault() throws Exception {
      test(DefaultCacheKeysFactory.SHORT_NAME, "CacheKeyImplementation");
   }

   @Test
   public void testDefaultClass() throws Exception {
      test(DefaultCacheKeysFactory.class.getName(), "CacheKeyImplementation");
   }

   @Test
   public void testSimple() throws Exception {
      test(SimpleCacheKeysFactory.SHORT_NAME, Name.class.getSimpleName());
   }

   @Test
   public void testSimpleClass() throws Exception {
      test(SimpleCacheKeysFactory.class.getName(), Name.class.getSimpleName());
   }

   private void test(String cacheKeysFactory, String keyClassName) throws Exception {
      SessionFactory sessionFactory = getSessionFactory(cacheKeysFactory);
      try {
         withTxSession(false, sessionFactory, s -> {
            Person person = new Person("John", "Black", 39);
            s.persist(person);
         });

         RegionFactory regionFactory = ((CacheImplementor) sessionFactory.getCache()).getRegionFactory();
         TestRegionFactory factory = TestRegionFactoryProvider.load().wrap(regionFactory);
         Cache<Object, Object> cache = factory.getCacheManager().getCache(Person.class.getName());
         Iterator<InternalCacheEntry<Object, Object>> iterator = cache.getAdvancedCache().getDataContainer().iterator();
         assertTrue(iterator.hasNext());
         Object key = iterator.next().getKey();
         assertEquals(keyClassName, key.getClass().getSimpleName());

         withTxSession(false, sessionFactory, s -> {
            Person person = s.load(Person.class, new Name("John", "Black"));
            assertEquals(39, person.getAge());
         });
      } finally {
         sessionFactory.close();
      }
   }
}
