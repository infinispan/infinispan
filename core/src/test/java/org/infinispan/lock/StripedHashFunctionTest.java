package org.infinispan.lock;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.util.StripedHashFunction;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "lock.StripedHashFunctionTest")
public class StripedHashFunctionTest extends AbstractInfinispanTest {
   private StripedHashFunction<String> stripedHashFunction;

   @BeforeMethod
   public void setUp() {
      stripedHashFunction = new StripedHashFunction<>(500);
   }

   public void testHashingDistribution() {
      // ensure even bucket distribution of lock stripes
      List<String> keys = createRandomKeys(1000);

      Map<Integer, Integer> distribution = new HashMap<>();

      for (String s : keys) {
         int segmentIndex = stripedHashFunction.hashToSegment(s);
         log.tracef("Lock for %s is %s", s, segmentIndex);
         if (distribution.containsKey(segmentIndex)) {
            int count = distribution.get(segmentIndex) + 1;
            distribution.put(segmentIndex, count);
         } else {
            distribution.put(segmentIndex, 1);
         }
      }

      // cannot be larger than the number of locks
      log.trace("dist size: " + distribution.size());
      log.trace("num shared locks: " + stripedHashFunction.getNumSegments());
      assert distribution.size() <= stripedHashFunction.getNumSegments();
      // assume at least a 2/3rd spread
      assert distribution.size() * 1.5 >= stripedHashFunction.getNumSegments();
   }

   private List<String> createRandomKeys(int number) {

      List<String> f = new ArrayList<>(number);
      int i = number;
      while (f.size() < number) {
         String s = i + "baseKey" + (10000 + i++);
         f.add(s);
      }

      return f;
   }
}
