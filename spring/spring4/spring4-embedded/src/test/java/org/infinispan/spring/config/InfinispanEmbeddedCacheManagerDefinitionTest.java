package org.infinispan.spring.config;


import org.infinispan.spring.test.InfinispanTestExecutionListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cache.CacheManager;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author Marius Bogoevici
 */

@Test(groups = {"functional", "smoke"}, testName = "spring.config.InfinispanEmbeddedCacheManagerDefinitionTest")
@ContextConfiguration
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@TestExecutionListeners(InfinispanTestExecutionListener.class)
public class InfinispanEmbeddedCacheManagerDefinitionTest extends AbstractTestNGSpringContextTests {

   @Autowired
   @Qualifier("cacheManager")
   private CacheManager embeddedCacheManager;

   @Autowired
   @Qualifier("withConfigFile")
   private CacheManager embeddedCacheManagerWithConfigFile;

   public void testEmbeddedCacheManagerExists() {
      Assert.assertNotNull(embeddedCacheManager);
      Assert.assertNotNull(embeddedCacheManagerWithConfigFile);
   }
}
