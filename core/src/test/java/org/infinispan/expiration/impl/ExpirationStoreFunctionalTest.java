package org.infinispan.expiration.impl;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "expiration.impl.ExpirationStoreFunctionalTest")
public class ExpirationStoreFunctionalTest extends ExpirationFunctionalTest {

   @Override
   protected void configure(ConfigurationBuilder config) {
      config.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class);
   }
}
