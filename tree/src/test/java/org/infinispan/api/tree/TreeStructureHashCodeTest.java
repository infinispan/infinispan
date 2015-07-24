package org.infinispan.api.tree;

import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.tree.Fqn;
import org.infinispan.tree.impl.NodeKey;
import org.infinispan.util.concurrent.locks.impl.InfinispanLock;
import org.infinispan.util.concurrent.locks.impl.StripedLockContainer;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests the degree to which hash codes get spread
 */
@Test(groups = "unit", testName = "api.tree.TreeStructureHashCodeTest")
public class TreeStructureHashCodeTest {

   public void testHashCodesAppendedCount() {
      List<Fqn> fqns = new ArrayList<>();
      fqns.add(Fqn.ROOT);
      for (int i = 0; i < 256; i++) fqns.add(Fqn.fromString("/fqn" + i));
      doTest(fqns);
   }

   public void testHashCodesAlpha() {
      List<Fqn> fqns = new ArrayList<>();
      fqns.add(Fqn.ROOT);
      for (int i = 0; i < 256; i++) fqns.add(Fqn.fromString("/" + Integer.toString(i, 36)));
      doTest(fqns);
   }

   private void doTest(List<Fqn> fqns) {
      StripedLockContainer container = new StripedLockContainer(512, AnyEquivalence.getInstance());
      container.inject(AbstractInfinispanTest.TIME_SERVICE);
      Map<InfinispanLock, Integer> distribution = new HashMap<>();
      for (Fqn f : fqns) {
         NodeKey dataKey = new NodeKey(f, NodeKey.Type.DATA);
         NodeKey structureKey = new NodeKey(f, NodeKey.Type.STRUCTURE);
         addToDistribution(container.getLock(dataKey), distribution);
         addToDistribution(container.getLock(structureKey), distribution);
      }

      assert distribution.size() <= container.size() : "Cannot have more locks than the container size!";
      // assume at least a 2/3rd even distribution
      // but also consider that data snd structure keys would typically provide the same hash code
      // so we need to double this
      assert distribution.size() * 1.5 * 2 >= container.size() : "Poorly distributed!  Distribution size is just " + distribution.size() + " and there are " + container.size() + " shared locks";

   }

   private void addToDistribution(InfinispanLock lock, Map<InfinispanLock, Integer> map) {
      int count = 1;
      if (map.containsKey(lock)) count = map.get(lock) + 1;
      map.put(lock, count);
   }
}
