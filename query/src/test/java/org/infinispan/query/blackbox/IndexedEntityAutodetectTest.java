package org.infinispan.query.blackbox;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import org.hibernate.search.engine.integration.impl.ExtendedSearchIntegrator;
import org.hibernate.search.engine.spi.EntityIndexBinding;
import org.hibernate.search.spi.SearchIntegrator;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.test.AnotherGrassEater;
import org.infinispan.query.test.Person;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Tests that undeclared indexed entities are autodetected.
 *
 * @author anistor@redhat.com
 * @since 8.2
 */
@Test(groups = {"functional", "smoke"}, testName = "query.blackbox.IndexedEntityAutodetectTest")
public class IndexedEntityAutodetectTest extends LocalCacheTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      // this configuration does not declare any indexed types on purpose, so they are autodetected
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg
            .indexing()
            .index(Index.ALL)
            .addProperty("default.directory_provider", "ram")
            .addProperty("error_handler", "org.infinispan.query.helper.StaticTestingErrorHandler")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      enhanceConfig(cfg);
      return TestCacheManagerFactory.createCacheManager(cfg);
   }

   /**
    * Verifies if the indexing interceptor is aware of a specific list of types.
    *
    * @param cache the cache containing the indexes
    * @param types vararg listing the types the indexing engine should know
    */
   private void assertIndexingKnows(Cache<Object, Object> cache, Class<?>... types) {
      ComponentRegistry cr = cache.getAdvancedCache().getComponentRegistry();
      SearchIntegrator searchIntegrator = cr.getComponent(SearchIntegrator.class);
      assertNotNull(searchIntegrator);
      Map<Class<?>, EntityIndexBinding> indexBindingForEntity = searchIntegrator.unwrap(ExtendedSearchIntegrator.class).getIndexBindings();
      assertNotNull(indexBindingForEntity);
      Set<Class<?>> keySet = indexBindingForEntity.keySet();
      assertEquals(types.length, keySet.size());
      assertTrue(keySet.containsAll(Arrays.asList(types)));
   }

   @Override
   protected void loadTestingData() {
      assertIndexingKnows(cache);

      super.loadTestingData();

      assertIndexingKnows(cache, Person.class, AnotherGrassEater.class);
   }

   public void testEntityDiscovery() {
      assertIndexingKnows(cache);

      Person p = new Person();
      p.setName("Lucene developer");
      p.setAge(30);
      p.setBlurb("works best on weekends");
      cache.put(p.getName(), p);

      assertIndexingKnows(cache, Person.class);
   }
}
