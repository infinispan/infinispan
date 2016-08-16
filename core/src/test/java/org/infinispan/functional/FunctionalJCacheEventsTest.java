package org.infinispan.functional;

import static org.infinispan.functional.FunctionalListenerAssertions.assertNoEvents;
import static org.infinispan.functional.FunctionalListenerAssertions.assertOrderedEvents;
import static org.infinispan.functional.FunctionalListenerAssertions.assertUnorderedEvents;
import static org.infinispan.functional.FunctionalListenerAssertions.create;
import static org.infinispan.functional.FunctionalListenerAssertions.createAllRemoveAll;
import static org.infinispan.functional.FunctionalListenerAssertions.createModify;
import static org.infinispan.functional.FunctionalListenerAssertions.createModifyRemove;
import static org.infinispan.functional.FunctionalListenerAssertions.createRemove;
import static org.infinispan.functional.FunctionalListenerAssertions.createThenRemove;
import static org.infinispan.functional.FunctionalListenerAssertions.write;
import static org.infinispan.functional.FunctionalListenerAssertions.writeValueNull;

import org.testng.annotations.Test;

@Test(groups = "functional", testName = "functional.FunctionalJCacheEventsTest")
public class FunctionalJCacheEventsTest extends FunctionalJCacheTest {

   @Override
   public void testLocalEmptyGetThenPut() {
      assertOrderedEvents(local2, super::testLocalEmptyGetThenPut, write("one"));
   }

   @Override
   public void testReplEmptyGetThenPutOnNonOwner() {
      assertOrderedEvents(repl2, super::testReplEmptyGetThenPutOnNonOwner, write("one"));
   }

   @Override
   public void testReplEmptyGetThenPutOnOwner() {
      assertOrderedEvents(repl2, super::testReplEmptyGetThenPutOnOwner, write("one"));
   }

   @Override
   public void testDistEmptyGetThenPutOnNonOwner() {
      assertNoEvents(dist2, super::testDistEmptyGetThenPutOnNonOwner);
   }

   @Override
   public void testDistEmptyGetThenPutOnOwner() {
      assertOrderedEvents(dist2, super::testDistEmptyGetThenPutOnOwner, write("one"));
   }

   @Override
   public void testLocalPutGet() {
      assertOrderedEvents(local2, super::testLocalPutGet, create("one"));
   }

   @Override
   public void testReplPutGetOnNonOwner() {
      assertOrderedEvents(repl2, super::testReplPutGetOnNonOwner, create("one"));
   }

   @Override
   public void testReplPutGetOnOwner() {
      assertOrderedEvents(repl2, super::testReplPutGetOnOwner, create("one"));
   }

   @Override
   public void testDistPutGetOnNonOwner() {
      assertNoEvents(dist2, super::testDistPutGetOnNonOwner);
   }

   @Override
   public void testDistPutGetOnOwner() {
      assertOrderedEvents(dist2, super::testDistPutGetOnOwner, create("one"));
   }

   @Override
   public void testLocalGetAndPut() {
      assertOrderedEvents(local2, super::testLocalGetAndPut, createModify("one", "uno"));
   }

   @Override
   public void testReplGetAndPutOnNonOwner() {
      assertOrderedEvents(repl2, super::testReplGetAndPutOnNonOwner, createModify("one", "uno"));
   }

   @Override
   public void testReplGetAndPutOnOwner() {
      assertOrderedEvents(repl2, super::testReplGetAndPutOnOwner, createModify("one", "uno"));
   }

   @Override
   public void testDistGetAndPutOnNonOwner() {
      assertNoEvents(dist2, super::testDistGetAndPutOnNonOwner);
   }

   @Override
   public void testDistGetAndPutOnOwner() {
      assertOrderedEvents(dist2, super::testDistGetAndPutOnOwner, createModify("one", "uno"));
   }

   @Override
   public void testLocalGetAndRemove() {
      assertOrderedEvents(local2, super::testLocalGetAndRemove, createThenRemove("one", "two"));
   }

   @Override
   public void testReplGetAndRemoveOnNonOwner() {
      assertOrderedEvents(repl2, super::testReplGetAndRemoveOnNonOwner, createThenRemove("one", "two"));
   }

   @Override
   public void testReplGetAndRemoveOnOwner() {
      assertOrderedEvents(repl2, super::testReplGetAndRemoveOnOwner, createThenRemove("one", "two"));
   }

   @Override
   public void testDistGetAndRemoveOnNonOwner() {
      assertNoEvents(dist2, super::testDistGetAndRemoveOnNonOwner);
   }

   @Override
   public void testDistGetAndRemoveOnOwner() {
      assertOrderedEvents(dist2, super::testDistGetAndRemoveOnOwner, createThenRemove("one", "two"));
   }

   @Override
   public void testLocalContainsKey() {
      assertOrderedEvents(local2, super::testLocalContainsKey, create("one"));
   }

   @Override
   public void testReplContainsKeyOnNonOwner() {
      assertOrderedEvents(repl2, super::testReplContainsKeyOnNonOwner, create("one"));
   }

   @Override
   public void testReplContainsKeyOnOwner() {
      assertOrderedEvents(repl2, super::testReplContainsKeyOnOwner, create("one"));
   }

   @Override
   public void testDistContainsKeyOnNonOwner() {
      assertNoEvents(dist2, super::testDistContainsKeyOnNonOwner);
   }

   @Override
   public void testDistContainsKeyOnOwner() {
      assertOrderedEvents(dist2, super::testDistContainsKeyOnOwner, create("one"));
   }

   @Override
   public void testLocalClear() {
      assertUnorderedEvents(local2, super::testLocalClear, write("one", "two", "two"));
   }

   @Override
   public void testReplClearOnNonOwner() {
      assertUnorderedEvents(repl2, super::testReplClearOnNonOwner, write("one", "two", "two"));
   }

   @Override
   public void testReplClearOnOwner() {
      assertUnorderedEvents(repl2, super::testReplClearOnOwner, write("one", "two", "two"));
   }

   @Override
   public void testDistClearOnNonOwner() {
      assertNoEvents(dist2, super::testDistClearOnNonOwner);
   }

   @Override
   public void testDistClearOnOwner() {
      assertUnorderedEvents(dist2, super::testDistClearOnOwner, write("one", "two", "two"));
   }

   @Override
   public void testLocalPutIfAbsent() {
      assertOrderedEvents(local2, super::testLocalPutIfAbsent, createRemove("one"));
   }

   @Override
   public void testReplPutIfAbsentOnNonOwner() {
      assertOrderedEvents(repl2, super::testReplPutIfAbsentOnNonOwner, createRemove("one"));
   }

   @Override
   public void testReplPutIfAbsentOnOwner() {
      assertOrderedEvents(repl2, super::testReplPutIfAbsentOnOwner, createRemove("one"));
   }

   @Override
   public void testDistPutIfAbsentOnNonOwner() {
      assertNoEvents(dist2, super::testDistPutIfAbsentOnNonOwner);
   }

   @Override
   public void testDistPutIfAbsentOnOwner() {
      assertOrderedEvents(dist2, super::testDistPutIfAbsentOnOwner, createRemove("one"));
   }

   @Override
   public void testLocalConditionalRemove() {
      assertOrderedEvents(local2, super::testLocalConditionalRemove, createRemove("one"));
   }

   @Override
   public void testReplConditionalRemoveOnNonOwner() {
      assertOrderedEvents(repl2, super::testReplConditionalRemoveOnNonOwner, createRemove("one"));
   }

   @Override
   public void testReplConditionalRemoveOnOwner() {
      assertOrderedEvents(repl2, super::testReplConditionalRemoveOnOwner, createRemove("one"));
   }

   @Override
   public void testDistConditionalRemoveOnNonOwner() {
      assertNoEvents(dist2, super::testDistConditionalRemoveOnNonOwner);
   }

   @Override
   public void testDistConditionalRemoveOnOwner() {
      assertOrderedEvents(dist2, super::testDistConditionalRemoveOnOwner, createRemove("one"));
   }

   @Override
   public void testLocalReplace() {
      assertOrderedEvents(local2, super::testLocalReplace, createModifyRemove("one", "uno"));
   }

   @Override
   public void testReplReplaceOnNonOwner() {
      assertOrderedEvents(repl2, super::testReplReplaceOnNonOwner, createModifyRemove("one", "uno"));
   }

   @Override
   public void testReplReplaceOnOwner() {
      assertOrderedEvents(repl2, super::testReplReplaceOnOwner, createModifyRemove("one", "uno"));
   }

   @Override
   public void testDistReplaceOnNonOwner() {
      assertNoEvents(dist2, super::testDistReplaceOnNonOwner);
   }

   @Override
   public void testDistReplaceOnOwner() {
      assertOrderedEvents(dist2, super::testDistReplaceOnOwner, createModifyRemove("one", "uno"));
   }

   @Override
   public void testLocalGetAndReplace() {
      assertOrderedEvents(local2, super::testLocalGetAndReplace, createModifyRemove("one", "uno"));
   }

   @Override
   public void testReplGetAndReplaceOnNonOwner() {
      assertOrderedEvents(repl2, super::testReplGetAndReplaceOnNonOwner, createModifyRemove("one", "uno"));
   }

   @Override
   public void testReplGetAndReplaceOnOwner() {
      assertOrderedEvents(repl2, super::testReplGetAndReplaceOnOwner, createModifyRemove("one", "uno"));
   }

   @Override
   public void testDistGetAndReplaceOnNonOwner() {
      assertNoEvents(repl2, super::testDistGetAndReplaceOnNonOwner);
   }

   @Override
   public void testDistGetAndReplaceOnOwner() {
      assertOrderedEvents(dist2, super::testDistGetAndReplaceOnOwner, createModifyRemove("one", "uno"));
   }

   @Override
   public void testLocalReplaceWithValue() {
      assertOrderedEvents(local2, super::testLocalReplaceWithValue, createModifyRemove("one", "uno"));
   }

   @Override
   public void testReplReplaceWithValueOnNonOwner() {
      assertOrderedEvents(repl2, super::testReplReplaceWithValueOnNonOwner, createModifyRemove("one", "uno"));
   }

   @Override
   public void testReplReplaceWithValueOnOwner() {
      assertOrderedEvents(repl2, super::testReplReplaceWithValueOnOwner, createModifyRemove("one", "uno"));
   }

   @Override
   public void testDistReplaceWithValueOnNonOwner() {
      assertNoEvents(dist2, super::testDistReplaceWithValueOnNonOwner);
   }

   @Override
   public void testDistReplaceWithValueOnOwner() {
      assertOrderedEvents(dist2, super::testDistReplaceWithValueOnOwner, createModifyRemove("one", "uno"));
   }

   @Override
   public void testLocalPutAll() {
      assertUnorderedEvents(local2, super::testLocalPutAll,
         writeValueNull("one", "two", "three", "four", "five", "five"));
   }

   @Override
   public void testReplPutAllOnNonOwner() {
      assertUnorderedEvents(repl2, super::testReplPutAllOnNonOwner,
         writeValueNull("one", "two", "three", "four", "five", "five"));
   }

   @Override
   public void testReplPutAllOnOwner() {
      assertUnorderedEvents(repl2, super::testReplPutAllOnOwner,
         writeValueNull("one", "two", "three", "four", "five", "five"));
   }

   @Override
   public void testDistPutAllOnNonOwner() {
      assertNoEvents(dist2, super::testDistPutAllOnNonOwner);
   }

   @Override
   public void testDistPutAllOnOwner() {
      assertUnorderedEvents(dist2, super::testDistPutAllOnOwner,
         writeValueNull("one", "two", "three", "four", "five", "five"));
   }

   @Override
   public void testLocalIterator() {
      assertUnorderedEvents(local2, super::testLocalIterator,
         write("one", "two", "three", "four", "five", "five"));
   }

   @Override
   public void testReplIteratorOnNonOwner() {
      assertUnorderedEvents(repl2, super::testReplIteratorOnNonOwner,
         write("one", "two", "three", "four", "five", "five"));
   }

   @Override
   public void testReplIteratorOnOwner() {
      assertUnorderedEvents(repl2, super::testReplIteratorOnOwner,
         write("one", "two", "three", "four", "five", "five"));
   }

   @Override
   public void testDistIteratorOnNonOwner() {
      assertNoEvents(dist2, super::testDistIteratorOnNonOwner);
   }

   @Override
   public void testDistIteratorOnOwner() {
      assertUnorderedEvents(dist2, super::testDistIteratorOnOwner,
         write("one", "two", "three", "four", "five", "five"));
   }

   @Override
   public void testLocalInvoke() {
      assertOrderedEvents(local2, super::testLocalInvoke, createRemove("one"));
   }

   @Override
   public void testReplInvokeOnNonOwner() {
      assertOrderedEvents(repl2, super::testReplInvokeOnNonOwner, createRemove("one"));
   }

   @Override
   public void testReplInvokeOnOwner() {
      assertOrderedEvents(repl2, super::testReplInvokeOnOwner, createRemove("one"));
   }

   @Override
   public void testDistInvokeOnNonOwner() {
      assertNoEvents(dist2, super::testDistInvokeOnNonOwner);
   }

   @Override
   public void testDistInvokeOnOwner() {
      assertOrderedEvents(dist2, super::testDistInvokeOnOwner, createRemove("one"));
   }

   @Override
   public void testLocalInvokeAll() {
      assertUnorderedEvents(local2, super::testLocalInvokeAll, createAllRemoveAll("one", "two", "three"));
   }

   @Override
   public void testReplInvokeAllOnNonOwner() {
      assertUnorderedEvents(repl2, super::testReplInvokeAllOnNonOwner, createAllRemoveAll("one", "two", "three"));
   }

   @Override
   public void testReplInvokeAllOnOwner() {
      assertUnorderedEvents(repl2, super::testReplInvokeAllOnOwner, createAllRemoveAll("one", "two", "three"));
   }

   @Override
   public void testDistInvokeAllOnNonOwner() {
      assertNoEvents(dist2, super::testDistInvokeAllOnNonOwner);
   }

   @Override
   public void testDistInvokeAllOnOwner() {
      assertUnorderedEvents(dist2, super::testDistInvokeAllOnOwner, createAllRemoveAll("one", "two", "three"));
   }

}
