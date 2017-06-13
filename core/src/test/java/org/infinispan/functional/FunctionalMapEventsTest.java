package org.infinispan.functional;

import static org.infinispan.functional.FunctionalListenerAssertions.TestEvent;
import static org.infinispan.functional.FunctionalListenerAssertions.assertNoEvents;
import static org.infinispan.functional.FunctionalListenerAssertions.assertOrderedEvents;
import static org.infinispan.functional.FunctionalListenerAssertions.assertUnorderedEvents;
import static org.infinispan.functional.FunctionalListenerAssertions.create;
import static org.infinispan.functional.FunctionalListenerAssertions.createModify;
import static org.infinispan.functional.FunctionalListenerAssertions.write;
import static org.infinispan.functional.FunctionalListenerAssertions.writeModify;
import static org.infinispan.functional.FunctionalListenerAssertions.writeRemove;
import static org.infinispan.functional.FunctionalTestUtils.rw;
import static org.infinispan.functional.FunctionalTestUtils.wo;

import java.util.Arrays;
import java.util.Collection;

import org.infinispan.functional.decorators.FunctionalListeners;
import org.infinispan.functional.impl.FunctionalMapImpl;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "functional.FunctionalMapEventsTest")
public class FunctionalMapEventsTest extends FunctionalMapTest {

   LocalFunctionalListeners<Integer> localL2;
   LocalFunctionalListeners<Object> replL2;
   LocalFunctionalListeners<Object> distL2;

   @BeforeClass
   @Override
   public void createBeforeClass() throws Throwable {
      super.createBeforeClass();
      localL2 = new LocalFunctionalListeners<>(fmapL2);
      replL2 = new LocalFunctionalListeners<>(fmapR2);
      distL2 = new LocalFunctionalListeners<>(fmapD2);
   }

   @Override
   public void testLocalWriteConstantAndReadGetsValue() {
      assertOrderedEvents(localL2, super::testLocalWriteConstantAndReadGetsValue, write("one"));
   }

   @Override
   public void testReplWriteConstantAndReadGetsValueOnNonOwner() {
      assertOrderedEvents(replL2, super::testReplWriteConstantAndReadGetsValueOnNonOwner, write("one"));
   }

   @Override
   public void testReplWriteConstantAndReadGetsValueOnOwner() {
      assertOrderedEvents(replL2, super::testReplWriteConstantAndReadGetsValueOnOwner, write("one"));
   }

   @Override
   public void testDistWriteConstantAndReadGetsValueOnNonOwner() {
      assertNoEvents(distL2, super::testDistWriteConstantAndReadGetsValueOnNonOwner);
   }

   @Override
   public void testDistWriteConstantAndReadGetsValueOnOwner() {
      assertOrderedEvents(distL2, super::testDistWriteConstantAndReadGetsValueOnOwner, write("one"));
   }

   @Override
   public void testLocalWriteValueAndReadValueAndMetadata() {
      assertOrderedEvents(localL2, super::testLocalWriteValueAndReadValueAndMetadata, write("one"));
   }

   @Override
   public void testReplWriteValueAndReadValueAndMetadataOnNonOwner() {
      assertOrderedEvents(replL2, super::testReplWriteValueAndReadValueAndMetadataOnNonOwner, write("one"));
   }

   @Override
   public void testReplWriteValueAndReadValueAndMetadataOnOwner() {
      assertOrderedEvents(replL2, super::testReplWriteValueAndReadValueAndMetadataOnOwner, write("one"));
   }

   @Override
   public void testDistWriteValueAndReadValueAndMetadataOnNonOwner() {
      assertNoEvents(distL2, super::testDistWriteValueAndReadValueAndMetadataOnNonOwner);
   }

   @Override
   public void testDistWriteValueAndReadValueAndMetadataOnOwner() {
      assertOrderedEvents(distL2, super::testDistWriteValueAndReadValueAndMetadataOnOwner, write("one"));
   }

   @Override
   public void testLocalReadWriteGetsEmpty() {
      assertNoEvents(localL2, super::testLocalReadWriteGetsEmpty);
   }

   @Override
   public void testReplReadWriteGetsEmptyOnNonOwner() {
      assertNoEvents(replL2, super::testReplReadWriteGetsEmptyOnNonOwner);
   }

   @Override
   public void testReplReadWriteGetsEmptyOnOwner() {
      assertNoEvents(replL2, super::testReplReadWriteGetsEmptyOnOwner);
   }

   @Override
   public void testDistReadWriteGetsEmptyOnNonOwner() {
      assertNoEvents(distL2, super::testDistReadWriteGetsEmptyOnNonOwner);
   }

   @Override
   public void testDistReadWriteGetsEmptyOnOwner() {
      assertNoEvents(distL2, super::testDistReadWriteGetsEmptyOnOwner);
   }

   @Override
   public void testLocalReadWriteValuesReturnPrevious() {
      assertOrderedEvents(localL2, super::testLocalReadWriteValuesReturnPrevious, create("one"));
   }

   @Override
   public void testReplReadWriteValuesReturnPreviousOnNonOwner() {
      assertOrderedEvents(replL2, super::testReplReadWriteValuesReturnPreviousOnNonOwner, create("one"));
   }

   @Override
   public void testReplReadWriteValuesReturnPreviousOnOwner() {
      assertOrderedEvents(replL2, super::testReplReadWriteValuesReturnPreviousOnOwner, create("one"));
   }

   @Override
   public void testDistReadWriteValuesReturnPreviousOnNonOwner() {
      assertNoEvents(distL2, super::testDistReadWriteValuesReturnPreviousOnNonOwner);
   }

   @Override
   public void testDistReadWriteValuesReturnPreviousOnOwner() {
      assertOrderedEvents(distL2, super::testDistReadWriteValuesReturnPreviousOnOwner, create("one"));
   }

   @Override
   public void testLocalReadWriteForConditionalParamBasedReplace() {
      Collection<TestEvent<String>> events = createUpdateCreate();
      assertOrderedEvents(localL2, super::testLocalReadWriteForConditionalParamBasedReplace, events);
   }

   @Override
   public void testReplReadWriteForConditionalParamBasedReplaceOnNonOwner() {
      assertOrderedEvents(replL2, super::testReplReadWriteForConditionalParamBasedReplaceOnNonOwner, createUpdateCreate());
   }

   @Override
   public void testReplReadWriteForConditionalParamBasedReplaceOnOwner() {
      assertOrderedEvents(replL2, super::testReplReadWriteForConditionalParamBasedReplaceOnOwner, createUpdateCreate());
   }

   @Override
   public void testDistReadWriteForConditionalParamBasedReplaceOnNonOwner() {
      assertNoEvents(distL2, super::testDistReadWriteForConditionalParamBasedReplaceOnNonOwner);
   }

   @Override
   public void testDistReadWriteForConditionalParamBasedReplaceOnOwner() {
      assertOrderedEvents(distL2, super::testDistReadWriteForConditionalParamBasedReplaceOnOwner, createUpdateCreate());
   }

   @Override
   public void testLocalReadOnlyEvalManyEmpty() {
      assertNoEvents(localL2, super::testLocalReadOnlyEvalManyEmpty);
   }

   @Override
   public void testReplReadOnlyEvalManyEmptyOnNonOwner() {
      assertNoEvents(replL2, super::testReplReadOnlyEvalManyEmptyOnNonOwner);
   }

   @Override
   public void testReplReadOnlyEvalManyEmptyOnOwner() {
      assertNoEvents(replL2, super::testReplReadOnlyEvalManyEmptyOnOwner);
   }

   @Override
   public void testDistReadOnlyEvalManyEmptyOnNonOwner() {
      assertNoEvents(distL2, super::testDistReadOnlyEvalManyEmptyOnNonOwner);
   }

   @Override
   public void testDistReadOnlyEvalManyEmptyOnOwner() {
      assertNoEvents(distL2, super::testDistReadOnlyEvalManyEmptyOnOwner);
   }

   @Override
   public void testLocalUpdateSubsetAndReturnPrevs() {
      assertUnorderedEvents(localL2, super::testLocalUpdateSubsetAndReturnPrevs,
         writeModify(Arrays.asList("one", "two", "three"), Arrays.asList("bat", "bi", "hiru")));
   }

   @Override
   public void testReplUpdateSubsetAndReturnPrevsOnNonOwner() {
      assertUnorderedEvents(replL2, super::testReplUpdateSubsetAndReturnPrevsOnNonOwner,
         writeModify(Arrays.asList("one", "two", "three"), Arrays.asList("bat", "bi", "hiru")));
   }

   @Override
   public void testReplUpdateSubsetAndReturnPrevsOnOwner() {
      assertUnorderedEvents(replL2, super::testReplUpdateSubsetAndReturnPrevsOnOwner,
         writeModify(Arrays.asList("one", "two", "three"), Arrays.asList("bat", "bi", "hiru")));
   }

   @Override
   public void testDistUpdateSubsetAndReturnPrevsOnNonOwner() {
      assertNoEvents(distL2, super::testDistUpdateSubsetAndReturnPrevsOnNonOwner);
   }

   @Override
   public void testDistUpdateSubsetAndReturnPrevsOnOwner() {
      assertUnorderedEvents(distL2, super::testDistUpdateSubsetAndReturnPrevsOnOwner,
         writeModify(Arrays.asList("one", "two", "three"), Arrays.asList("bat", "bi", "hiru")));
   }

   @Override
   public void testLocalReadWriteToRemoveAllAndReturnPrevs() {
      assertUnorderedEvents(localL2, super::testLocalReadWriteToRemoveAllAndReturnPrevs,
         writeRemove("one", "two", "three"));
   }

   @Override
   public void testReplReadWriteToRemoveAllAndReturnPrevsOnNonOwner() {
      assertUnorderedEvents(replL2, super::testReplReadWriteToRemoveAllAndReturnPrevsOnNonOwner,
         writeRemove("one", "two", "three"));
   }

   @Override
   public void testReplReadWriteToRemoveAllAndReturnPrevsOnOwner() {
      assertUnorderedEvents(replL2, super::testReplReadWriteToRemoveAllAndReturnPrevsOnOwner,
         writeRemove("one", "two", "three"));
   }

   @Override
   public void testDistReadWriteToRemoveAllAndReturnPrevsOnNonOwner() {
      assertNoEvents(distL2, super::testDistReadWriteToRemoveAllAndReturnPrevsOnNonOwner);
   }

   @Override
   public void testDistReadWriteToRemoveAllAndReturnPrevsOnOwner() {
      assertUnorderedEvents(distL2, super::testDistReadWriteToRemoveAllAndReturnPrevsOnOwner,
         writeRemove("one", "two", "three"));
   }

   static Collection<TestEvent<String>> createUpdateCreate() {
      Collection<TestEvent<String>> events = createModify("one", "uno");
      events.addAll(create("one"));
      return events;
   }

   private static final class LocalFunctionalListeners<K> implements FunctionalListeners<K, String> {
      private final FunctionalMapImpl<K, String> fmap;

      private LocalFunctionalListeners(FunctionalMapImpl<K, String> fmap) {
         this.fmap = fmap;
      }

      @Override
      public Listeners.ReadWriteListeners<K, String> readWriteListeners() {
         return rw(fmap).listeners();
      }

      @Override
      public Listeners.WriteListeners<K, String> writeOnlyListeners() {
         return wo(fmap).listeners();
      }
   }

}
