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
@Test(groups = "unstable", testName = "spring.config.InfinispanContainerCacheManagerDefinitionTest",
      description = "Disabled temporarily, see https://issues.jboss.org/browse/ISPN-2701 -- original group: functional")
@ContextConfiguration
public class InfinispanContainerCacheManagerDefinitionTest extends AbstractTestNGSpringContextTests {

    @Autowired @Qualifier("cacheManager")
    private CacheManager containerCacheManager;

    @Autowired @Qualifier("cacheManager2")
    private CacheManager containerCacheManager2;

    public void testContainerCacheManagerExists() {
       Assert.assertNotNull(containerCacheManager);
       Assert.assertNotNull(containerCacheManager2);
    }
}
