package org.infinispan.query.dsl.embedded;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * @author anistor@redhat.com
 * @since 8.0
 */
@Test(groups = "functional", testName = "query.dsl.embedded.NonIndexedListenerWithDslFilterTest")
public class NonIndexedListenerWithDslFilterTest extends ListenerWithDslFilterTest {

   @Override
   protected ConfigurationBuilder getConfigurationBuilder() {
      return new ConfigurationBuilder();
   }
}
