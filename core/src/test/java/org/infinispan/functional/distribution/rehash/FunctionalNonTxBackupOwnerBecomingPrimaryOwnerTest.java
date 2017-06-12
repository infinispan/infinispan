package org.infinispan.functional.distribution.rehash;

import static org.infinispan.container.versioning.InequalVersionComparisonResult.EQUAL;

import org.infinispan.AdvancedCache;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.functional.FunctionalMap.ReadWriteMap;
import org.infinispan.functional.FunctionalMap.WriteOnlyMap;
import org.infinispan.functional.MetaParam.MetaEntryVersion;
import org.infinispan.distribution.rehash.NonTxBackupOwnerBecomingPrimaryOwnerTest;
import org.infinispan.distribution.rehash.TestWriteOperation;
import org.infinispan.functional.FunctionalTestUtils;
import org.infinispan.functional.decorators.FunctionalAdvancedCache;
import org.infinispan.functional.impl.FunctionalMapImpl;
import org.infinispan.functional.impl.ReadWriteMapImpl;
import org.infinispan.functional.impl.WriteOnlyMapImpl;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "distribution.rehash.FunctionalNonTxBackupOwnerBecomingPrimaryOwnerTest")
@CleanupAfterMethod
public class FunctionalNonTxBackupOwnerBecomingPrimaryOwnerTest extends NonTxBackupOwnerBecomingPrimaryOwnerTest {

   // TODO: Add more tests, e.g. read-write key operation

   <K, V> WriteOnlyMap<K, V> wo(int i) {
      FunctionalMapImpl<K, V> impl = FunctionalMapImpl.create(advancedCache(i));
      return WriteOnlyMapImpl.create(impl);
   }

   static <K, V> ReadWriteMap<K, V> rw(AdvancedCache<K, V> cache) {
      FunctionalMapImpl<K, V> impl = FunctionalMapImpl.create(cache);
      return ReadWriteMapImpl.create(impl);
   }

   @Override
   protected <A, B> AdvancedCache<A, B> advancedCache(int i) {
      AdvancedCache<A, B> cache = super.advancedCache(i);
      return FunctionalAdvancedCache.create(cache);
   }

   public void testPrimaryOwnerChangingDuringReplaceBasedOnMeta() throws Exception {
      // TODO: Move initial set and replace with meta logic to TestWriteOperation
      WriteOnlyMap<String, String> wo0 = wo(0);
      wo0.eval("testkey", wo -> wo.set("v0", new MetaEntryVersion(new NumericVersion(1))));
      doTest(TestWriteOperation.REPLACE_META_FUNCTIONAL);
   }

   @Override
   protected Object perform(TestWriteOperation op, AdvancedCache<Object, Object> cache0, String key) {
      try {
         return super.perform(op, cache0, key);
      } catch (IllegalArgumentException e) {
         switch (op) {
            case REPLACE_META_FUNCTIONAL:
               return FunctionalTestUtils.await(rw(cache0).eval(key, "v1", (v, rw) -> {

                     return rw.findMetaParam(MetaEntryVersion.class)
                        .filter(ver -> ver.get().compareTo(new NumericVersion(1)) == EQUAL)
                        .map(ver -> {
                           rw.set(v, new MetaEntryVersion(new NumericVersion(2)));
                           return true;
                        }).orElse(false);
                  }));
            default:
               throw new AssertionError("Unknown operation: " + op);
         }
      }
   }

   @Override
   public void testPrimaryOwnerChangingDuringPut() throws Exception {
      doTest(TestWriteOperation.PUT_CREATE_FUNCTIONAL);
   }

   @Override
   public void testPrimaryOwnerChangingDuringPutOverwrite() throws Exception {
      doTest(TestWriteOperation.PUT_OVERWRITE_FUNCTIONAL);
   }

   @Override
   public void testPrimaryOwnerChangingDuringPutIfAbsent() throws Exception {
      doTest(TestWriteOperation.PUT_IF_ABSENT_FUNCTIONAL);
   }

   @Override
   public void testPrimaryOwnerChangingDuringReplace() throws Exception {
      doTest(TestWriteOperation.REPLACE_FUNCTIONAL);
   }

   @Override
   public void testPrimaryOwnerChangingDuringRemove() throws Exception {
      doTest(TestWriteOperation.REMOVE_FUNCTIONAL);
   }

   @Override
   public void testPrimaryOwnerChangingDuringReplaceExact() throws Exception {
      doTest(TestWriteOperation.REPLACE_EXACT_FUNCTIONAL);
   }

   @Override
   public void testPrimaryOwnerChangingDuringRemoveExact() throws Exception {
      doTest(TestWriteOperation.REMOVE_EXACT_FUNCTIONAL);
   }

}
