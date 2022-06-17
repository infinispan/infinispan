package org.infinispan.persistence;

import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.marshall.TestObjectStreamMarshaller;
import org.infinispan.marshall.persistence.PersistenceMarshaller;
import org.infinispan.marshall.persistence.impl.MarshalledEntryUtil;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.NonBlockingStore;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.persistence.support.WaitDelegatingNonBlockingStore;
import org.infinispan.persistence.support.WaitNonBlockingStore;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests if the tombstones are properly stored and retrieved from a cache store.
 *
 * @since 14.0
 */
@Test(groups = "functional", testName = "persistence.BaseTombstonePersistenceTest")
public abstract class BaseTombstonePersistenceTest extends AbstractInfinispanTest {

   private WaitNonBlockingStore<String, String> store;
   private IntSet segments;
   private PersistenceMarshaller marshaller;

   @BeforeMethod(alwaysRun = true)
   public void setUp() throws Exception {
      marshaller = new TestObjectStreamMarshaller();
      segments = IntSets.immutableRangeSet(numSegments());
      try {
         store = getStore();
      } catch (Exception e) {
         log.error("Error creating store", e);
         throw e;
      }
   }

   //alwaysRun = true otherwise, when we run unstable tests, this method is not invoked (because it belongs to the unit group)
   @AfterMethod(alwaysRun = true)
   public void tearDown() throws PersistenceException {
      try {
         if (store != null) {
            store.clearAndWait();
            store.destroyAndWait();
         }
      } finally {
         store = null;
      }
   }

   protected static <E1, E2> WaitNonBlockingStore<E1, E2> wrapAndStart(NonBlockingStore<E1, E2> store, InitializationContext context) {
      WaitNonBlockingStore<E1, E2> waitStore = new WaitDelegatingNonBlockingStore<>(store, context.getKeyPartitioner());
      waitStore.startAndWait(context);
      return waitStore;
   }

   protected abstract WaitNonBlockingStore<String, String> getStore() throws Exception;

   protected PersistenceMarshaller getMarshaller() {
      return marshaller;
   }

   public void testTombstoneStream() {
      List<MarshallableEntry<String, String>> entries = createEntries(5);
      List<MarshallableEntry<String, String>> tombstones = createTombstones(10);

      entries.forEach(e -> assertFalse(e.isTombstone()));
      tombstones.forEach(e -> assertTrue(e.isTombstone()));

      entries.forEach(e -> store.write(e));
      tombstones.forEach(e -> store.write(e));

      List<MarshallableEntry<String, String>> stream = store.publishEntriesWait(segments, s -> true, true);

      assertContainsAll(stream, entries, true);
      assertContainsAll(stream, tombstones, true);

      stream = store.publishEntriesWait(segments, s -> true, false);

      assertContainsAll(stream, entries, false);
      assertContainsAll(stream, tombstones, true);

      List<String> keysStream = store.publishKeysWait(segments, s -> true);

      assertContainsAllKeys(keysStream, entries);
      if (keysStreamContainsTombstones()) {
         assertContainsAllKeys(keysStream, tombstones);
      } else {
         assertNotContainsAllKeys(keysStream, tombstones);
      }
   }

   public void testWriteAndLoad() {
      List<MarshallableEntry<String, String>> entries = createEntries(2);
      List<MarshallableEntry<String, String>> tombstones = createTombstones(2);

      entries.forEach(e -> store.write(e));
      tombstones.forEach(e -> store.write(e));

      entries.forEach(e -> assertTrue(isSame(e, store.loadEntry(e.getKey()), true)));
      tombstones.forEach(e -> assertTrue(isSame(e, store.loadEntry(e.getKey()), true)));
   }

   private List<MarshallableEntry<String, String>> createEntries(int nEntries) {
      List<MarshallableEntry<String, String>> entries = new ArrayList<>(nEntries);
      for (int i = 0; i < nEntries; ++i) {
         entries.add(createEntry("key-" + i, "value-" + i));
      }
      return entries;
   }

   private List<MarshallableEntry<String, String>> createTombstones(int nTombstones) {
      List<MarshallableEntry<String, String>> tombstones = new ArrayList<>(nTombstones);
      for (int i = 0; i < nTombstones; ++i) {
         tombstones.add(createTombstone("tomb-" + i, createTombstoneMetadata(i)));
      }
      return tombstones;
   }

   private MarshallableEntry<String, String> createEntry(String key, String value) {
      return MarshalledEntryUtil.create(key, value, marshaller);
   }

   private static PrivateMetadata createTombstoneMetadata(int version) {
      return new PrivateMetadata.Builder().entryVersion(new NumericVersion(version)).tombstone(true).build();
   }

   private MarshallableEntry<String, String> createTombstone(String key, PrivateMetadata metadata) {
      return MarshalledEntryUtil.create(key, metadata, marshaller);
   }

   private static void assertContainsAll(List<MarshallableEntry<String, String>> entries, List<MarshallableEntry<String, String>> subset, boolean checkValues) {
      for (MarshallableEntry<String, String> me : subset) {
         for (MarshallableEntry<String, String> other : entries) {
            if (isSame(me, other, checkValues)) {
               return;
            }
         }
         fail(me + " missing from " + entries);
      }
   }

   private static void assertContainsAllKeys(List<String> entries, List<MarshallableEntry<String, String>> subset) {
      for (MarshallableEntry<String, String> me : subset) {
         assertTrue(me + " missing from " + entries, entries.contains(me.getKey()));
      }
   }

   private static void assertNotContainsAllKeys(List<String> entries, List<MarshallableEntry<String, String>> subset) {
      for (MarshallableEntry<String, String> me : subset) {
         assertFalse(me + " found in " + entries, entries.contains(me.getKey()));
      }
   }

   private static boolean isSame(MarshallableEntry<String, String> one, MarshallableEntry<String, String> two, boolean checkValues) {
      boolean same = one != null && two != null &&
            Objects.equals(one.getKey(), two.getKey()) &&
            Objects.equals(one.getInternalMetadata(), two.getInternalMetadata());
      return same && (!checkValues || Objects.equals(one.getValue(), two.getValue()));
   }

   protected static int numSegments() {
      return 1;
   }

   protected abstract boolean keysStreamContainsTombstones();

}
