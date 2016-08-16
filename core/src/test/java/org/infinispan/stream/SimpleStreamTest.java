package org.infinispan.stream;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Test(groups = "functional", testName = "streams.LocalStreamTest")
public class SimpleStreamTest extends LocalStreamTest {
   @Override
   protected void enhanceConfiguration(ConfigurationBuilder builder) {
      builder.simpleCache(true);
   }
}
