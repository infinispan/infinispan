package org.infinispan.util;

import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

import java.util.Iterator;
import java.util.Map;

@Test(groups = "unit", testName = "util.BidirectionalLinkedHashMapTest")
public class BidirectionalLinkedHashMapTest extends AbstractInfinispanTest {
   public void testIterators() {
      BidirectionalLinkedHashMap<Integer, Object> map = new BidirectionalLinkedHashMap<Integer, Object>();
      initMap(map);

      testOrderBeforeRemoval(map);

      // Remove some stuff and check that the order is still in tact
      for (int i = 500; i < 600; i++) map.remove(i);

      testOrderAfterRemoval(map);

      // now attempt a visit and test that the visits are NOT recorded
      map.get(200);

      // order should still be maintained
      testOrderAfterRemoval(map);
   }

   public void testAccessOrderIterators() {
      BidirectionalLinkedHashMap<Integer, Object> map = new BidirectionalLinkedHashMap<Integer, Object>(
            BidirectionalLinkedHashMap.DEFAULT_INITIAL_CAPACITY,
            BidirectionalLinkedHashMap.DEFAULT_LOAD_FACTOR,
            true
      );
      initMap(map);

      testOrderBeforeRemoval(map);

      // now attempt a visit and test that the visits are NOT recorded
      map.get(200);

      // check the forward iterator that everything is in order of entry
      Iterator<Integer> it = map.keySet().iterator();
      int index = 0;
      while (it.hasNext()) {
         if (index == 200) index = 201;
         if (index == 1000) index = 200;
         assert it.next() == index++;
      }

      // now check the reverse iterator.
      it = map.keySet().reverseIterator();
      index = 200; // this should be the first
      while (it.hasNext()) {
         assert it.next() == index--;
         if (index == 199) index = 999;
         if (index == 200) index = 199;
      }
   }


   private void initMap(Map<Integer, Object> map) {
      Object value = new Object();
      for (int i = 0; i < 1000; i++) map.put(i, value);
   }

   private void testOrderBeforeRemoval(BidirectionalLinkedHashMap<Integer, Object> map) {
      // check the forward iterator that everything is in order of entry
      Iterator<Integer> it = map.keySet().iterator();
      int index = 0;
      while (it.hasNext()) assert it.next() == index++;

      assert index == 1000 : "Was expecting 1000, was " + index;

      // now check the reverse iterator.
      System.out.println("Keys: " + map.keySet());
      it = map.keySet().reverseIterator();
      index = 999;
      while (it.hasNext()) {
         int expected = index--;
         int actual = it.next();
         assert actual == expected : "Was expecting " + expected + " but was " + actual;
      }

      assert index == -1 : "Was expecting -1, was " + index;
   }


   private void testOrderAfterRemoval(BidirectionalLinkedHashMap<Integer, Object> map) {
      Iterator<Integer> it = map.keySet().iterator();
      int index = 0;
      while (it.hasNext()) {
         if (index == 500) index = 600; // 500 - 599 have been removed
         assert it.next() == index++;
      }

      // now check the reverse iterator.
      it = map.keySet().reverseIterator();
      index = 999;
      while (it.hasNext()) {
         if (index == 599) index = 499; // 500 - 599 have been removed
         assert it.next() == index--;
      }
   }
}
