package org.infinispan.spring.provider.sample;

import org.infinispan.spring.provider.SpringEmbeddedCacheManager;
import org.infinispan.spring.provider.sample.service.CachedBookService;
import org.infinispan.spring.test.InfinispanTestExecutionListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cache.CacheManager;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.testng.annotations.Test;

/**
 * Tests using embedded cache manager.
 *
 * @author Matej Cimbora (mcimbora@redhat.com)
 */
@Test(testName = "spring.provider.SampleEmbeddedCacheTest", groups = "functional", sequential = true)
@ContextConfiguration(locations = "classpath:/org/infinispan/spring/provider/sample/SampleEmbeddedCacheTestConfig.xml")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@TestExecutionListeners(InfinispanTestExecutionListener.class)
public class SampleEmbeddedCacheTest extends AbstractTestTemplateJsr107 {

   @Qualifier(value = "cachedBookServiceImpl")
   @Autowired(required = true)
   private CachedBookService bookService;

   @Autowired(required = true)
   private SpringEmbeddedCacheManager cacheManager;

   @Override
   public CachedBookService getBookService() {
      return bookService;
   }

   @Override
   public CacheManager getCacheManager() {
      return cacheManager;
   }
}
