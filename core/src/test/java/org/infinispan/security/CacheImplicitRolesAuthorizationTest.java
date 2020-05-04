package org.infinispan.security;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.LockingMode;
import org.testng.annotations.Test;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 11.0
 **/
@Test(groups = {"functional", "smoke"}, testName = "security.CacheImplicitRolesAuthorizationTest")
public class CacheImplicitRolesAuthorizationTest extends CacheAuthorizationTest {

   protected ConfigurationBuilder createCacheConfiguration(GlobalConfigurationBuilder global) {
      final ConfigurationBuilder config = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      config.transaction().lockingMode(LockingMode.PESSIMISTIC);
      config.invocationBatching().enable();
      config.security().authorization().enable();
      return config;
   }
}
