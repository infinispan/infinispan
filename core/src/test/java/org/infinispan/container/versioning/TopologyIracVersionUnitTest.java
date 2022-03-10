package org.infinispan.container.versioning;

import static org.testng.Assert.assertNotEquals;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertSame;

import java.util.stream.IntStream;

import org.infinispan.container.versioning.irac.TopologyIracVersion;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

/**
 * Unit test for {@link TopologyIracVersion}
 *
 * @since 14.0
 */
@Test(groups = "unit", testName = "container.versioning.TopologyIracVersionUnitTest")
public class TopologyIracVersionUnitTest extends AbstractInfinispanTest {

   public void testNoVersionInstance() {
      assertSame(TopologyIracVersion.NO_VERSION, TopologyIracVersion.create(0, 0));
   }

   public void testIncrement() {
      TopologyIracVersion vBase = TopologyIracVersion.create(10, 1);
      // < topology
      assertEquals(vBase.increment(5), TopologyIracVersion.create(10, 2));
      // == topology
      assertEquals(vBase.increment(10), TopologyIracVersion.create(10, 2));
      // > topology
      assertEquals(vBase.increment(15), TopologyIracVersion.create(15, 1));
   }

   public void testNoVersionIncrement() {
      assertEquals(TopologyIracVersion.NO_VERSION.increment(2), TopologyIracVersion.create(2, 1));
   }

   public void testCompare() {
      TopologyIracVersion vBase = TopologyIracVersion.create(5, 5);

      // < topology and < version ; == version ; > version
      IntStream.range(4, 7).forEach(value -> assertCompare(vBase, TopologyIracVersion.create(4, value), 1));

      // == topology ; < version
      assertCompare(vBase, TopologyIracVersion.create(5, 4), 1);
      // == topology ; == version
      assertCompareEquals(vBase, TopologyIracVersion.create(5, 5));
      // == topology ; > version
      assertCompare(vBase, TopologyIracVersion.create(5, 6), -1);

      // > topology and < version ; == version ; > version
      IntStream.range(4, 7).forEach(value -> assertCompare(vBase, TopologyIracVersion.create(6, value), -1));
   }

   public void testMax() {
      TopologyIracVersion vBase = TopologyIracVersion.create(5, 5);

      // < topology and < version ; == version ; > version
      IntStream.range(4, 7).forEach(value -> assertMax(vBase, TopologyIracVersion.create(4, value), true));

      // == topology ; < version
      assertMax(vBase, TopologyIracVersion.create(5, 4), true);
      // == topology ; == version
      assertMax(vBase, TopologyIracVersion.create(5, 5), true);
      // == topology ; > version
      assertMax(vBase, TopologyIracVersion.create(5, 6), false);

      // > topology and < version ; == version ; > version
      IntStream.range(4, 7).forEach(value -> assertMax(vBase, TopologyIracVersion.create(6, value), false));
   }

   private static void assertCompare(TopologyIracVersion v1, TopologyIracVersion v2, int result) {
      assertEquals(result, Integer.signum(v1.compareTo(v2)));
      assertEquals(result * -1, Integer.signum(v2.compareTo(v1)));
      assertNotEquals(v1, v2);
      assertNotEquals(v2, v1);
   }

   private static void assertCompareEquals(TopologyIracVersion v1, TopologyIracVersion v2) {
      assertEquals(0, v1.compareTo(v2));
      assertEquals(0, v2.compareTo(v1));
      assertEquals(v1, v2);
      assertEquals(v2, v1);
   }

   private static void assertMax(TopologyIracVersion v1, TopologyIracVersion v2, boolean expectsV1) {
      assertEquals(expectsV1 ? v1 : v2, TopologyIracVersion.max(v1, v2));
      assertEquals(expectsV1 ? v1 : v2, TopologyIracVersion.max(v2, v1));
   }

}
