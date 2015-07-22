package org.infinispan.expiration.impl;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.ControlledTimeService;
import org.infinispan.util.TimeService;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static org.testng.AssertJUnit.assertEquals;

@Test(groups = "functional", testName = "expiration.impl.ExpirationStoreFunctionalTest")
public class ExpirationStoreFunctionalTest extends ExpirationFunctionalTest {

   @Override
   protected void configure(ConfigurationBuilder config) {
      config.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class);
   }
}
