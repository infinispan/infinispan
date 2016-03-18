package org.infinispan.expiration.impl;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.file.SingleFileStore;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "expiration.impl.ExpirationSingleFileStoreListenerFunctionalTest")
public class ExpirationSingleFileStoreListenerFunctionalTest extends ExpirationStoreListenerFunctionalTest {
   @Override
   protected void configure(ConfigurationBuilder config) {
      config
              // Prevent the reaper from running, reaperEnabled(false) doesn't work when a store is present
              .expiration().wakeUpInterval(Long.MAX_VALUE)
              .persistence().addSingleFileStore();
   }
}
