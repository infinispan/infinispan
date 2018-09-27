package org.infinispan.query.backend;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import org.hibernate.search.backend.spi.WorkType;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.impl.ComponentRegistryUtils;
import org.infinispan.query.impl.DefaultSearchWorkCreator;
import org.infinispan.query.test.Person;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * @author gustavonalle
 * @since 7.0
 */
@Test(groups = "functional", testName = "query.backend.CustomSearchWorkCreatorTest")
public class CustomSearchWorkCreatorTest extends SingleCacheManagerTest {

   @Test
   @SuppressWarnings("unchecked")
   public void testCustomWorkCreator() {
      DefaultSearchWorkCreator customSearchWorkCreator = spy(new DefaultSearchWorkCreator());
      QueryInterceptor queryInterceptor = ComponentRegistryUtils.getQueryInterceptor(cache);
      queryInterceptor.setSearchWorkCreator(customSearchWorkCreator);
      KeyTransformationHandler keyTransformationHandler = queryInterceptor.getKeyTransformationHandler();

      Integer key = 1;
      Person value = new Person("john", "blurb", 30);
      cache.put(key, value);

      verify(customSearchWorkCreator).createPerEntityWorks(value, keyTransformationHandler.keyToString(key), WorkType.UPDATE);
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(false);
      cfg.indexing().index(Index.ALL)
              .addIndexedEntity(Person.class)
              .addProperty("default.directory_provider", "local-heap")
              .addProperty("lucene_version", "LUCENE_CURRENT");
      return TestCacheManagerFactory.createCacheManager(cfg);
   }
}
