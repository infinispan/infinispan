package org.infinispan.objectfilter.test;

import org.infinispan.objectfilter.impl.ReflectionMatcher;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public class ReflectionMatcherTest extends AbstractMatcherTest {

   protected ReflectionMatcher createMatcher() {
      return new ReflectionMatcher(null);
   }
}
