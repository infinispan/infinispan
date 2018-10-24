package org.infinispan.expiration.impl;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "expiration.impl.ExpirationStoreFunctionalTest")
public class ExpirationStoreFunctionalTest extends ExpirationFunctionalTest {

   @Factory
   @Override
   public Object[] factory() {
      return new Object[]{
            // Test is for dummy store and we don't care about memory storage types
            new ExpirationStoreFunctionalTest(),
      };
   }

   @Override
   protected String parameters() {
      return null;
   }

   @Override
   protected void configure(ConfigurationBuilder config) {
      config
              // Prevent the reaper from running, reaperEnabled(false) doesn't work when a store is present
              .expiration().wakeUpInterval(Long.MAX_VALUE)
              .persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class);
   }
}
