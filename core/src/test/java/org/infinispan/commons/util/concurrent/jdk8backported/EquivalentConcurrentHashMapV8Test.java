package org.infinispan.commons.util.concurrent.jdk8backported;

import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.commons.equivalence.ByteArrayEquivalence;
import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.util.concurrent.BoundedConcurrentHashMapTest;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.testng.AssertJUnit.*;

/**
 * Verifies that the {@link EquivalentConcurrentHashMapV8}'s customizations
 * for equals and hashCode callbacks are working as expected. In other words,
 * if byte arrays are used as key/value types, the equals and hashCode
 * calculations are done based on their contents and not on their standard
 * JDK behaviour.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
@Test(groups = "functional", testName = "util.concurrent.jdk8backported.EquivalentConcurrentHashMapV8Test")
public class EquivalentConcurrentHashMapV8Test extends BoundedConcurrentHashMapTest {

   private static final Log log = LogFactory.getLog(EquivalentConcurrentHashMapV8Test.class);

   public void testByteArrayComputeIfAbsent() {
      byteArrayComputeIfAbsent(createComparingConcurrentMap());
   }

   public void testByteArrayComputeIfPresent() {
      byteArrayComputeIfPresent(createComparingConcurrentMap());
   }

   public void testByteArrayMerge() {
      byteArrayMerge(createComparingConcurrentMap());
   }

   public void testByteArrayOperationsWithTreeHashBins() {
      // This test forces all entries to be stored under the same hash bin,
      // kicking off different logic for comparing keys.
      EquivalentConcurrentHashMapV8<byte[], byte[]> map =
            createComparingTreeHashBinsForceChm();
      // More data needs to be put so that tree bin construction kicks in
      for (byte b = 0; b < 20; b++)
         map.put(new byte[]{b}, new byte[]{0});

      // The bin should become a tree bin
      EquivalentConcurrentHashMapV8.Node<byte[], byte[]> tab =
            EquivalentConcurrentHashMapV8.tabAt(map.table, 1);
      assertNotNull(tab);
      assertTrue(tab instanceof EquivalentConcurrentHashMapV8.TreeBin);

      EquivalentConcurrentHashMapV8.TreeBin<byte[], byte[]> treeBin =
            (EquivalentConcurrentHashMapV8.TreeBin<byte[], byte[]>) tab;

      for (byte b = 0; b < 10; b++) {
         byte[] key = {b};
         byte[] value = treeBin.find(1, key).getValue();
         byte[] expected = {0};
         assertTrue(String.format(
               "Expected key=%s to return value=%s, instead returned %s", str(key), str(expected), str(value)),
               Arrays.equals(expected, value));
      }
   }

   public void testTreeHashBinNotLost() {
      ConcurrentMap<Object, KeyHolder<?>> map =
            new EquivalentConcurrentHashMapV8<Object, KeyHolder<?>>(
                  new EvilKeyEquivalence(), AnyEquivalence.<KeyHolder<?>>getInstance());

      final long seed = 1370014958369218000L;
      System.out.println("SEED: " + seed);
      final Random random = new Random(seed);

      final int ENTRIES = 10000;
      final EvilKey evilKey = new EvilKey("6137");
      for(int i = 0; i < ENTRIES; i++) {
         final Object o;
         switch(i % 4) {
            case 0:
               final int hashCode = random.nextInt();
               o = new Object() {
                  @Override
                  public int hashCode() {
                     return hashCode;
                  }
               };
               break;
            case 1:
               o = new EvilKey(Integer.toString(i));
               break;
            default:
               o = new EvilComparableKey(Integer.toString(i));

         }
         map.put(o, new KeyHolder<Object>(o));
      }

      log.info(map.get(evilKey));
      int lost = 0;
      for (Object o : map.keySet()) {
         final boolean b = map.get(o) == null;
         if(b) ++lost;
         assert !b : o;
         assert map.containsKey(o) : o;
      }
      log.info("Lost " + lost + " entries");
   }

   private void byteArrayComputeIfAbsent(
         EquivalentConcurrentHashMapV8<byte[], byte[]> map) {
      byte[] key = {1, 2, 3};
      byte[] value = {4, 5, 6};
      map.put(key, value);

      byte[] computeKey = {1, 2, 3}; // on purpose, different instance required
      byte[] newValue = map.computeIfAbsent(
            computeKey, new EquivalentConcurrentHashMapV8.Fun<byte[], byte[]>() {
         @Override
         public byte[] apply(byte[] bytes) {
            return new byte[]{7, 8, 9};
         }
      });

      // Old value should be present
      assertTrue(String.format(
            "Expected value=%s to be returned by operation, instead returned value=%s",
                  str(value), str(newValue)),
            Arrays.equals(newValue, value));
   }

   private void byteArrayComputeIfPresent(
         EquivalentConcurrentHashMapV8<byte[], byte[]> map) {
      byte[] key = {1, 2, 3};
      byte[] value = {4, 5, 6};
      map.put(key, value);

      byte[] computeKey = {1, 2, 3}; // on purpose, different instance required
      byte[] newValue = map.computeIfPresent(computeKey,
            new EquivalentConcurrentHashMapV8.BiFun<byte[], byte[], byte[]>() {
               @Override
               public byte[] apply(byte[] bytes, byte[] bytes2) {
                  return new byte[]{7, 8, 9};
               }
            }
      );

      byte[] expectedValue = {7, 8, 9};
      assertTrue(String.format(
            "Expected value=%s to be returned by operation, instead returned value=%s",
            str(expectedValue), str(newValue)),
            Arrays.equals(newValue, expectedValue));
   }

   private void byteArrayMerge(
         EquivalentConcurrentHashMapV8<byte[], byte[]> map) {
      byte[] key = {1, 2, 3};
      byte[] value = {4, 5, 6};
      map.put(key, value);

      byte[] computeKey = {1, 2, 3}; // on purpose, different instance required
      byte[] newValue = map.merge(computeKey, new byte[]{},
         new EquivalentConcurrentHashMapV8.BiFun<byte[], byte[], byte[]>() {
            @Override
            public byte[] apply(byte[] bytes, byte[] bytes2) {
               return new byte[]{7, 8, 9};
            }
         }
      );

      // Old value should be present
      byte[] expectedValue = {7, 8, 9};
      assertTrue(String.format(
            "Expected value=%s to be returned by operation, instead returned value=%s",
            str(expectedValue), str(newValue)),
            Arrays.equals(newValue, expectedValue));
   }

   @Override
   protected ConcurrentMap<byte[], byte[]> createStandardConcurrentMap() {
      return new ConcurrentHashMap<byte[], byte[]>();
   }

   @Override
   protected EquivalentConcurrentHashMapV8<byte[], byte[]> createComparingConcurrentMap() {
      return new EquivalentConcurrentHashMapV8<byte[], byte[]>(
            EQUIVALENCE, EQUIVALENCE);
   }

   private EquivalentConcurrentHashMapV8<byte[], byte[]> createComparingTreeHashBinsForceChm() {
      return new EquivalentConcurrentHashMapV8<byte[], byte[]>(
            2, new SameHashByteArray(), ByteArrayEquivalence.INSTANCE);
   }

   private static class SameHashByteArray implements Equivalence<byte[]> {

      @Override
      public int hashCode(Object obj) {
         return 1;
      }

      @Override
      public boolean equals(byte[] obj, Object otherObj) {
         return ByteArrayEquivalence.INSTANCE.equals(obj, otherObj);
      }

      @Override
      public String toString(Object obj) {
         return ByteArrayEquivalence.INSTANCE.toString(obj);
      }

      @Override
      public boolean isComparable(Object obj) {
         return ByteArrayEquivalence.INSTANCE.isComparable(obj);
      }

      @Override
      public int compare(byte[] obj, byte[] otherObj) {
         return ByteArrayEquivalence.INSTANCE.compare(obj, otherObj);
      }

   }

   private static class KeyHolder<K> {
      final K key;

      KeyHolder(final K key) {
         this.key = key;
      }
   }

   private static class EvilKey {
      final String value;

      EvilKey(final String value) {
         this.value = value;
      }

      @Override
      public int hashCode() {
         return this.value.hashCode() & 1;
      }

      @Override
      public boolean equals(final Object obj) {
         return obj != null && obj.getClass() == this.getClass() && ((EvilKey)obj).value.equals(value);
      }

      @Override
      public String toString() {
         return this.getClass().getSimpleName() + " ( \"" + value + "\" )";
      }

   }

   private static class EvilComparableKey extends EvilKey implements Comparable<EvilComparableKey> {

      EvilComparableKey(final String value) {
         super(value);
      }

      @Override
      public int compareTo(final EvilComparableKey o) {
         return value.compareTo(o != null ? o.value : null);
      }

   }

   private static class EvilKeyEquivalence implements Equivalence<Object> {

      @Override
      public int hashCode(Object obj) {
         return obj != null ? obj.hashCode() : 0;
      }

      @Override
      public boolean equals(Object obj, Object otherObj) {
         return obj != null && obj.equals(otherObj);
      }

      @Override
      public String toString(Object obj) {
         return obj.toString();
      }

      @Override
      public boolean isComparable(Object obj) {
         return obj instanceof EvilComparableKey;
      }

      @Override
      public int compare(Object obj, Object otherObj) {
         return obj instanceof EvilComparableKey && otherObj instanceof EvilComparableKey
               ? ((EvilComparableKey) obj).compareTo((EvilComparableKey) otherObj) : 0;
      }

   }

}
