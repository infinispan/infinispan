package org.infinispan.spring.config;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cache.CacheManager;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author Marius Bogoevici
 */

@Test(groups = "functional", testName = "spring.config.InfinispanRemoteCacheManagerDefinitionTest")
@ContextConfiguration
public class InfinispanRemoteCacheManagerDefinitionTest extends AbstractTestNGSpringContextTests {

   @Autowired
   @Qualifier("cacheManager")
   private CacheManager remoteCacheManager;

   @Autowired
   @Qualifier("withConfigFile")
   private CacheManager remoteCacheManagerWithConfigFile;

   @Test
   public void testRemoteCacheManagerExists() {
      Assert.assertNotNull(remoteCacheManager);
      Assert.assertNotNull(remoteCacheManagerWithConfigFile);
   }
}
