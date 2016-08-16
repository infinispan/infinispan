package org.infinispan.functional;

import static org.infinispan.functional.FunctionalListenerAssertions.assertNoEvents;
import static org.infinispan.functional.FunctionalListenerAssertions.assertOrderedEvents;
import static org.infinispan.functional.FunctionalListenerAssertions.assertUnorderedEvents;
import static org.infinispan.functional.FunctionalListenerAssertions.create;
import static org.infinispan.functional.FunctionalListenerAssertions.createAllRemoveAll;
import static org.infinispan.functional.FunctionalListenerAssertions.createModify;
import static org.infinispan.functional.FunctionalListenerAssertions.createModifyRemove;
import static org.infinispan.functional.FunctionalListenerAssertions.createRemove;
import static org.infinispan.functional.FunctionalListenerAssertions.write;
import static org.infinispan.functional.FunctionalListenerAssertions.writeRemove;

import java.util.Arrays;

import org.testng.annotations.Test;

@Test(groups = "functional", testName = "functional.FunctionalConcurrentMapEventsTest")
public class FunctionalConcurrentMapEventsTest extends FunctionalConcurrentMapTest {

   public void testLocalEmptyGetThenPut() {
      assertOrderedEvents(local2, super::testLocalEmptyGetThenPut, create("one"));
   }

   @Override
   public void testReplEmptyGetThenPutOnNonOwner() {
      assertOrderedEvents(repl2, super::testReplEmptyGetThenPutOnNonOwner, create("one"));
   }

   @Override
   public void testReplEmptyGetThenPutOnOwner() {
      assertOrderedEvents(repl2, super::testReplEmptyGetThenPutOnOwner, create("one"));
   }

   @Override
   public void testDistEmptyGetThenPutOnNonOwner() {
      assertNoEvents(dist2, super::testDistEmptyGetThenPutOnNonOwner);
   }

   @Override
   public void testDistEmptyGetThenPutOnOwner() {
      assertOrderedEvents(dist2, super::testDistEmptyGetThenPutOnOwner, create("one"));
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
   public void testLocalPutUpdate() {
      assertOrderedEvents(local2, super::testLocalPutUpdate, createModify("one", "uno"));
   }

   @Override
   public void testReplPutUpdateOnNonOwner() {
      assertOrderedEvents(repl2, super::testReplPutUpdateOnNonOwner, createModify("one", "uno"));
   }

   @Override
   public void testReplPutUpdateOnOwner() {
      assertOrderedEvents(repl2, super::testReplPutUpdateOnOwner, createModify("one", "uno"));
   }

   @Override
   public void testDistPutUpdateOnNonOwner() {
      assertNoEvents(dist2, super::testDistPutUpdateOnNonOwner);
   }

   @Override
   public void testDistPutUpdateOnOwner() {
      assertOrderedEvents(dist2, super::testDistPutUpdateOnOwner, createModify("one", "uno"));
   }

   @Override
   public void testLocalGetAndRemove() {
      assertOrderedEvents(local2, super::testLocalGetAndRemove, createRemove("one"));
   }

   @Override
   public void testReplGetAndRemoveOnNonOwner() {
      assertOrderedEvents(repl2, super::testReplGetAndRemoveOnNonOwner, createRemove("one"));
   }

   @Override
   public void testReplGetAndRemoveOnOwner() {
      assertOrderedEvents(repl2, super::testReplGetAndRemoveOnOwner, createRemove("one"));
   }

   @Override
   public void testDistGetAndRemoveOnNonOwner() {
      assertNoEvents(dist2, super::testDistGetAndRemoveOnNonOwner);
   }

   @Override
   public void testDistGetAndRemoveOnOwner() {
      assertOrderedEvents(dist2, super::testDistGetAndRemoveOnOwner, createRemove("one"));
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
   public void testLocalContainsValue() {
      assertOrderedEvents(local2, super::testLocalContainsValue, create("one"));
   }

   @Override
   public void testReplContainsValueOnNonOwner() {
      assertOrderedEvents(repl2, super::testReplContainsValueOnNonOwner, create("one"));
   }

   @Override
   public void testReplContainsValueOnOwner() {
      assertOrderedEvents(repl2, super::testReplContainsValueOnOwner, create("one"));
   }

   @Override
   public void testDistContainsValueOnNonOwner() {
      assertNoEvents(dist2, super::testDistContainsValueOnNonOwner);
   }

   @Override
   public void testDistContainsValueOnOwner() {
      assertOrderedEvents(dist2, super::testDistContainsValueOnOwner, create("one"));
   }

   @Override
   public void testLocalSize() {
      assertOrderedEvents(local2, super::testLocalSize, createAllRemoveAll("one", "two"));
   }

   @Override
   public void testReplSizeOnNonOwner() {
      assertOrderedEvents(repl2, super::testReplSizeOnNonOwner, createAllRemoveAll("one", "two"));
   }

   @Override
   public void testReplSizeOnOwner() {
      assertOrderedEvents(repl2, super::testReplSizeOnOwner, createAllRemoveAll("one", "two"));
   }

   @Override
   public void testDistSizeOnNonOwner() {
      assertNoEvents(dist2, super::testDistSizeOnNonOwner);
   }

   @Override
   public void testDistSizeOnOwner() {
      assertOrderedEvents(dist2, super::testDistSizeOnOwner, createAllRemoveAll("one", "two"));
   }

   @Override
   public void testLocalEmpty() {
      assertOrderedEvents(local2, super::testLocalEmpty, createRemove("one"));
   }

   @Override
   public void testReplEmptyOnNonOwner() {
      assertOrderedEvents(repl2, super::testReplEmptyOnNonOwner, createRemove("one"));
   }

   @Override
   public void testReplEmptyOnOwner() {
      assertOrderedEvents(repl2, super::testReplEmptyOnOwner, createRemove("one"));
   }

   @Override
   public void testDistEmptyOnNonOwner() {
      assertNoEvents(dist2, super::testDistEmptyOnNonOwner);
   }

   @Override
   public void testDistEmptyOnOwner() {
      assertOrderedEvents(dist2, super::testDistEmptyOnOwner, createRemove("one"));
   }

   @Override
   public void testLocalPutAll() {
      assertUnorderedEvents(local2, super::testLocalPutAll, writeRemove("one", "two", "two"));
   }

   @Override
   public void testReplPutAllOnNonOwner() {
      assertUnorderedEvents(repl2, super::testReplPutAllOnNonOwner, writeRemove("one", "two", "two"));
   }

   @Override
   public void testReplPutAllOnOwner() {
      assertUnorderedEvents(repl2, super::testReplPutAllOnOwner, writeRemove("one", "two", "two"));
   }

   @Override
   public void testDistPutAllOnNonOwner() {
      assertNoEvents(dist2, super::testDistPutAllOnNonOwner);
   }

   @Override
   public void testDistPutAllOnOwner() {
      assertUnorderedEvents(dist2, super::testDistPutAllOnOwner, writeRemove("one", "two", "two"));
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
   public void testLocalKeyValueAndEntrySets() {
      assertUnorderedEvents(local2, super::testLocalKeyValueAndEntrySets,
         createModifyRemove(Arrays.asList("one", "two", "two"), Arrays.asList("uno", "dos", "dos")));
   }

   @Override
   public void testReplKeyValueAndEntrySetsOnNonOwner() {
      assertUnorderedEvents(repl2, super::testReplKeyValueAndEntrySetsOnNonOwner,
         createModifyRemove(Arrays.asList("one", "two", "two"), Arrays.asList("uno", "dos", "dos")));
   }

   @Override
   public void testReplKeyValueAndEntrySetsOnOwner() {
      assertUnorderedEvents(repl2, super::testReplKeyValueAndEntrySetsOnOwner,
         createModifyRemove(Arrays.asList("one", "two", "two"), Arrays.asList("uno", "dos", "dos")));
   }

   @Override
   public void testDistKeyValueAndEntrySetsOnNonOwner() {
      assertNoEvents(dist2, super::testDistKeyValueAndEntrySetsOnNonOwner);
   }

   @Override
   public void testDistKeyValueAndEntrySetsOnOwner() {
      assertUnorderedEvents(dist2, super::testDistKeyValueAndEntrySetsOnOwner,
         createModifyRemove(Arrays.asList("one", "two", "two"), Arrays.asList("uno", "dos", "dos")));
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

}
