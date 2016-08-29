package org.infinispan.query.dsl.embedded;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.objectfilter.ParsingException;
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

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN028521: Full-text queries cannot be applied to property 'name' in type org.infinispan.query.test.Person unless the property is indexed and analyzed.")
   public void testDisallowFullTextQuery() {
      super.testDisallowFullTextQuery();
   }
}
