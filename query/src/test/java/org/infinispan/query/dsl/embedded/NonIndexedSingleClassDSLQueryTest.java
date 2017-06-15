package org.infinispan.query.dsl.embedded;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * Test entities defined as inner classes and inheritance of fields using non-indexed query. Just a simple field equals
 * is tested. The purpose of this test is just to check class and property lookup correctness.
 *
 * @author anistor@redhat.com
 * @since 9.1
 */
@Test(groups = "functional", testName = "query.dsl.embedded.NonIndexedSingleClassDSLQueryTest")
public class NonIndexedSingleClassDSLQueryTest extends SingleClassDSLQueryTest {

   @Override
   protected void configureCache(ConfigurationBuilder builder) {
      // do nothing
   }
}
