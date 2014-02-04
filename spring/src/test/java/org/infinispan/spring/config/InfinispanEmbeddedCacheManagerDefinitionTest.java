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

@Test(groups = "unstable", testName = "spring.config.InfinispanEmbeddedCacheManagerDefinitionTest",
      description = "Disabled temporarily, see https://issues.jboss.org/browse/ISPN-2701 -- original group: functional")
@ContextConfiguration
public class InfinispanEmbeddedCacheManagerDefinitionTest extends AbstractTestNGSpringContextTests {

    @Autowired @Qualifier("cacheManager")
    private CacheManager embeddedCacheManager;

    @Autowired @Qualifier("withConfigFile")
    private CacheManager embeddedCacheManagerWithConfigFile;

    public void testEmbeddedCacheManagerExists() {
       Assert.assertNotNull(embeddedCacheManager);
       Assert.assertNotNull(embeddedCacheManagerWithConfigFile);
    }
}
