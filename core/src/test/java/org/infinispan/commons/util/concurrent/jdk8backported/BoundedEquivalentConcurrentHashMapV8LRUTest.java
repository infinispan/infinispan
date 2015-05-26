package org.infinispan.commons.util.concurrent.jdk8backported;

import org.infinispan.commons.util.concurrent.jdk8backported.BoundedEquivalentConcurrentHashMapV8.Eviction;
import org.testng.annotations.Test;

/**
 * Tests bounded concurrent hash map V8 logic against the JDK ConcurrentHashMap.
 *
 * @author William Burns
 * @since 7.1
 */
@Test(groups = "functional", testName = "util.concurrent.BoundedEquivalentConcurrentHashMapV8LRUTest")
public class BoundedEquivalentConcurrentHashMapV8LRUTest extends BoundedEquivalentConcurrentHashMapV8BaseTest {
   @Override
   protected Eviction evictionPolicy() {
      return Eviction.LRU;
   }
}
