package org.infinispan.spring.embedded.provider.sample;

import org.infinispan.spring.embedded.provider.sample.service.CachedBookService;
import org.infinispan.spring.embedded.provider.SpringEmbeddedCacheManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cache.CacheManager;
import org.springframework.test.context.ContextConfiguration;
import org.testng.annotations.Test;

/**
 * Tests using XML based cache configuration.
 *
 * @author Matej Cimbora (mcimbora@redhat.com)
 */
@Test(testName = "spring.embedded.provider.SampleXmlConfigurationTest", groups = "functional", sequential = true)
@ContextConfiguration(locations = "classpath:/org/infinispan/spring/embedded/provider/sample/SampleXmlConfigurationTestConfig.xml")
public class SampleXmlConfigurationTest extends AbstractTestTemplate {

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
