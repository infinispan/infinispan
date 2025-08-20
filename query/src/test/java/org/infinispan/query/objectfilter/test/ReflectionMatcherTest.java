package org.infinispan.query.objectfilter.test;

import org.infinispan.query.objectfilter.impl.ReflectionMatcher;
import org.testng.annotations.Test;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
@Test(testName = "query.objectfilter.test.ReflectionMatcherTest")
public class ReflectionMatcherTest extends AbstractMatcherTest {

   protected ReflectionMatcher createMatcher() {
      return new ReflectionMatcher((ClassLoader) null);
   }

}
