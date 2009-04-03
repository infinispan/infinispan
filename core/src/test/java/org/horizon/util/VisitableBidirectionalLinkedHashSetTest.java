package org.horizon.util;

import org.testng.annotations.Test;

import java.util.Iterator;
import java.util.Set;

@Test(groups = "unit", testName = "util.VisitableBidirectionalLinkedHashSetTest")
public class VisitableBidirectionalLinkedHashSetTest {

   public void testVisitableSet() {
      VisitableBidirectionalLinkedHashSet<Integer> set = new VisitableBidirectionalLinkedHashSet<Integer>(true);
      initSet(set);

      testOrderBeforeRemoval(set);

      // now attemp a visit and test that the visits are NOT recorded
      set.visit(200);

      // check the forward iterator that everything is in order of entry
      Iterator<Integer> it = set.iterator();
      int index = 0;
      while (it.hasNext()) {
         if (index == 200) index = 201;
         if (index == 1000) index = 200;
         Integer value = it.next();
         assert value == index++ : "Expecting " + (index - 1) + " but was " + value;
      }

      // now check the reverse iterator.
      it = set.reverseIterator();
      index = 200; // this should be the first
      while (it.hasNext()) {
         assert it.next() == index--;
         if (index == 199) index = 999;
         if (index == 200) index = 199;
      }

      for (Iterator i = set.iterator(); i.hasNext();) {
         i.next();
         i.remove();
      }

      assert set.isEmpty();
      assert set.size() == 0 : "Expecting size to be 0 but was " + set.size();
   }


   private void initSet(Set<Integer> set) {
      for (int i = 0; i < 1000; i++) set.add(i);
   }

   private void testOrderBeforeRemoval(VisitableBidirectionalLinkedHashSet<Integer> set) {
      // check the forward iterator that everything is in order of entry
      Iterator<Integer> it = set.iterator();
      int index = 0;
      while (it.hasNext()) assert it.next() == index++;
      assert index == 1000 : "Expected 1000, index was " + index;
      assert set.size() == 1000;
      // now check the reverse iterator.
      it = set.reverseIterator();
      index = 999;
      while (it.hasNext()) {
         Integer value = it.next();
         assert value == index-- : "failed, expecting " + (index + 1) + " but was " + value;
      }
      assert index == -1 : "Was " + index + ", instead of -1";
   }
}
