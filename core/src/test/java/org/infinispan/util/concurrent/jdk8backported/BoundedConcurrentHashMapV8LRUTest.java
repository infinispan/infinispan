package org.infinispan.util.concurrent.jdk8backported;

import org.infinispan.util.concurrent.jdk8backported.BoundedConcurrentHashMapV8.Eviction;
import org.testng.annotations.Test;

/**
 * Tests bounded concurrent hash map V8 logic against the JDK ConcurrentHashMap.
 *
 * @author William Burns
 * @since 7.1
 */
@Test(groups = "functional", testName = "util.concurrent.BoundedConcurrentHashMapTest")
public class BoundedConcurrentHashMapV8LRUTest extends BoundedConcurrentHashMapV8BaseTest {
   @Override
   protected Eviction evictionPolicy() {
      return Eviction.LRU;
   }
}
