package org.infinispan.client.hotrod;

import java.util.concurrent.TimeUnit;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * This test verifies that an entry has the proper lifespan when a default value is applied
 *
 * @author William Burns
 * @since 8.0
 */
@Test(groups = "functional", testName = "client.hotrod.MixedExpiryLifespanDefaultTest")
public class MixedExpiryLifespanDefaultTest extends MixedExpiryTest {
   @Override
   protected void configure(ConfigurationBuilder configurationBuilder) {
      configurationBuilder.expiration().lifespan(3, TimeUnit.MINUTES);
   }
}
