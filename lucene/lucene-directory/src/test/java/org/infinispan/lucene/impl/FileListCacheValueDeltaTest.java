package org.infinispan.lucene.impl;

import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(groups = "unit", testName = "lucene.FileListCacheValueDeltaTest")
public class FileListCacheValueDeltaTest {

   @Test
   public void testDeltasWithAddAndRemove() {
      FileListCacheValue fileListCacheValue = new FileListCacheValue();
      fileListCacheValue.add("a");
      fileListCacheValue.add("b");
      fileListCacheValue.add("c");
      fileListCacheValue.remove("a");
      fileListCacheValue.remove("c");
      FileListCacheValueDelta delta = fileListCacheValue.delta();
      List<Operation> ops = delta.getOps();

      assertTrue(fileListCacheValue.toArray().length == 1);
      assertTrue(delta.getOps().size() == 1);
      assertAddOperation(ops.get(0), "b");
   }

   @Test
   public void testDeltasWithEmpty() {
      FileListCacheValue cacheValue = new FileListCacheValue();
      FileListCacheValueDelta delta = cacheValue.delta();

      assertTrue(delta.getOps().isEmpty());
   }

   @Test
   public void testSeveralDeltas() {
      FileListCacheValue cacheValue = new FileListCacheValue();
      cacheValue.add("_.cf1");
      cacheValue.add("_.cf2");
      cacheValue.add("_.cf3");
      FileListCacheValueDelta delta = cacheValue.delta();

      assertTrue(delta.getOps().size() == 3);
      assertAddOperation(delta.getOps().get(0), "_.cf1");
      assertAddOperation(delta.getOps().get(1), "_.cf2");
      assertAddOperation(delta.getOps().get(2), "_.cf3");

      cacheValue.remove("_.cf3");

      FileListCacheValueDelta anotherDelta = cacheValue.delta();
      assertTrue(anotherDelta.getOps().size() == 1);
      assertDeleteOperation(anotherDelta.getOps().get(0), "_.cf3");
   }

   @Test
   public void testAddRemove() {
      FileListCacheValue fileListCacheValue = new FileListCacheValue();
      fileListCacheValue.addAndRemove("string1", "string2");
      FileListCacheValueDelta delta = fileListCacheValue.delta();
      List<Operation> ops = delta.getOps();

      assertEquals(1, delta.getOps().size());
      assertAddOperation(ops.get(0), "string1");
   }

   @Test
   public void testCommit() {
      FileListCacheValue cacheValue = new FileListCacheValue();
      cacheValue.add("string1");
      cacheValue.add("string2");
      cacheValue.add("string3");
      cacheValue.commit();

      assertTrue(cacheValue.delta().getOps().isEmpty());
   }

   @Test
   public void testRemoveAddSameElement() {
      FileListCacheValue cacheValue = new FileListCacheValue();
      cacheValue.add("string1");
      cacheValue.add("string2");
      cacheValue.add("string3");
      cacheValue.remove("string1");
      cacheValue.remove("string3");
      FileListCacheValueDelta delta = cacheValue.delta();

      assertTrue(delta.getOps().size() == 1);
   }

   @Test
   public void testDeltasWithRepeatedChanges() {
      FileListCacheValue cacheValue = new FileListCacheValue();
      cacheValue.add("string1");
      cacheValue.add("string1");
      cacheValue.add("string1");
      FileListCacheValueDelta delta = cacheValue.delta();

      assertTrue(delta.getOps().size() == 1);
      assertAddOperation(delta.getOps().get(0), "string1");
   }

   @Test
   public void testRemoveNonexistent() {
      FileListCacheValue cacheValue = new FileListCacheValue();
      cacheValue.remove("2");
      FileListCacheValueDelta delta = cacheValue.delta();

      assertTrue(delta.getOps().isEmpty());
   }

   @Test
   public void testMerge() throws Exception {
      FileListCacheValue original = new FileListCacheValue();
      FileListCacheValue target = new FileListCacheValue();
      original.add("1");
      original.add("2");
      original.add("3");
      original.remove("4");
      original.remove("2");
      original.delta().merge(target);

      assertEquals(original, target);
   }

   private <T> void assertAddOperation(Object operation, T onElement) {
      assertTrue(AddOperation.class.isAssignableFrom(operation.getClass()));
      assertEquals(((AddOperation) operation).getElement(), onElement);
   }

   private <T> void assertDeleteOperation(Object operation, T onElement) {
      assertTrue(DeleteOperation.class.isAssignableFrom(operation.getClass()));
      assertEquals(((DeleteOperation) operation).getElement(), onElement);
   }


}
