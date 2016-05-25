package org.infinispan.security;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.CacheContainer;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.infinispan.test.fwk.TestCacheManagerFactory.createCacheManager;

@Test(groups="functional", testName="security.SecurityConfigurationTest")
public class SecurityConfigurationTest extends AbstractInfinispanTest {

   @Test(expectedExceptions = CacheConfigurationException.class,
         expectedExceptionsMessageRegExp = ".*ISPN000414.*")
   public void testIncompleteConfiguration() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.security().authorization().enable().role("reader");
      withCacheManager(() -> createCacheManager(builder), CacheContainer::getCache);
   }

}
