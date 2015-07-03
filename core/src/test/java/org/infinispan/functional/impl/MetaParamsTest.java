package org.infinispan.functional.impl;

import org.infinispan.commons.api.functional.EntryVersion.NumericEntryVersion;
import org.infinispan.commons.api.functional.MetaParam;
import org.infinispan.commons.api.functional.MetaParam.Created;
import org.infinispan.commons.api.functional.MetaParam.EntryVersionParam;
import org.infinispan.commons.api.functional.MetaParam.LastUsed;
import org.infinispan.commons.api.functional.MetaParam.Lifespan;
import org.infinispan.commons.api.functional.MetaParam.MaxIdle;
import org.testng.annotations.Test;

import java.util.NoSuchElementException;
import java.util.Optional;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

/**
 * Unit test for metadata parameters collection.
 */
@Test(groups = "functional", testName = "functional.impl.MetaParamsTest")
public class MetaParamsTest {

   public void testEmptyMetaParamsFind() {
      MetaParams metas = MetaParams.empty();
      assertTrue(metas.isEmpty());
      assertEquals(0, metas.size());
      assertFalse(metas.find(Lifespan.class).isPresent());
      assertFalse(metas.find(EntryVersionParam.class).isPresent());
      assertFalse(metas.find(MaxIdle.class).isPresent());
   }

   @Test(expectedExceptions = NoSuchElementException.class)
   public void testEmptyMetaParamsGet() {
      MetaParams metas = MetaParams.empty();
      assertTrue(metas.isEmpty());
      assertEquals(0, metas.size());
      metas.get(Lifespan.class);
   }

   @Test
   public void testAddFindMetaParam() {
      MetaParams metas = MetaParams.empty();
      Lifespan lifespan = new Lifespan(1000);
      metas.add(lifespan);
      assertFalse(metas.isEmpty());
      assertEquals(1, metas.size());
      Lifespan lifespanFound = metas.get(Lifespan.class);
      assertEquals(new Lifespan(1000), lifespanFound);
      assertEquals(1000, metas.get(Lifespan.class).get().longValue());
      assertFalse(new Lifespan(900).equals(lifespanFound));
      metas.add(new Lifespan(900));
      assertFalse(metas.isEmpty());
      assertEquals(1, metas.size());
      assertEquals(new Lifespan(900), metas.get(lifespan.getClass()));
   }

   @Test
   public void testAddFindMultipleMetaParams() {
      MetaParams metas = MetaParams.empty();
      metas.addMany(new Lifespan(1000), new MaxIdle(1000), new EntryVersionParam<>(new NumericEntryVersion(12345)));
      assertFalse(metas.isEmpty());
      assertEquals(3, metas.size());
      MaxIdle maxIdle = metas.get(MaxIdle.class);
      EntryVersionParam<Long> entryVersion = metas.get(EntryVersionParam.getType());
      assertEquals(new MaxIdle(1000), maxIdle);
      assertFalse(900 == maxIdle.get().longValue());
      assertEquals(new EntryVersionParam<>(new NumericEntryVersion(12345)), entryVersion);
      assertFalse(new EntryVersionParam<>(new NumericEntryVersion(23456)).equals(entryVersion));
   }

   @Test
   public void testReplaceFindMultipleMetaParams() {
      MetaParams metas = MetaParams.empty();
      metas.addMany(new Lifespan(1000), new MaxIdle(1000), new EntryVersionParam<>(new NumericEntryVersion(12345)));
      assertFalse(metas.isEmpty());
      assertEquals(3, metas.size());
      metas.addMany(new Lifespan(2000), new MaxIdle(2000));
      assertFalse(metas.isEmpty());
      assertEquals(3, metas.size());
      assertEquals(Optional.of(new MaxIdle(2000)), metas.find(MaxIdle.class));
      assertEquals(Optional.of(new Lifespan(2000)), metas.find(Lifespan.class));
      assertEquals(Optional.of(new EntryVersionParam<>(new NumericEntryVersion(12345))),
         metas.find(EntryVersionParam.class));
   }

   @Test
   public void testConstructors() {
      MetaParams metasOf1 = MetaParams.of(new Created(1000));
      assertFalse(metasOf1.isEmpty());
      assertEquals(1, metasOf1.size());
      MetaParams metasOf2 = MetaParams.of(new Created(1000), new LastUsed(2000));
      assertFalse(metasOf2.isEmpty());
      assertEquals(2, metasOf2.size());
      MetaParams metasOf4 = MetaParams.of(
         new Created(1000), new LastUsed(2000), new Lifespan(3000), new MaxIdle(4000));
      assertFalse(metasOf4.isEmpty());
      assertEquals(4, metasOf4.size());
   }

   // TODO
   @Test(enabled = false)
   public void testDuplicateParametersOnConstruction() {
      EntryVersionParam<Long> versionParam1 = new EntryVersionParam<>(new NumericEntryVersion(100));
      EntryVersionParam<Long> versionParam2 = new EntryVersionParam<>(new NumericEntryVersion(200));
      MetaParams metas = MetaParams.of(versionParam1, versionParam2);
      assertEquals(1, metas.size());
   }

   @Test
   public void testDuplicateParametersOnAdd() {
      EntryVersionParam<Long> versionParam1 = new EntryVersionParam<>(new NumericEntryVersion(100));
      MetaParams metas = MetaParams.of(versionParam1);
      assertEquals(1, metas.size());
      assertEquals(new EntryVersionParam<>(new NumericEntryVersion(100)), metas.get(EntryVersionParam.getType()));

      EntryVersionParam<Long> versionParam2 = new EntryVersionParam<>(new NumericEntryVersion(200));
      metas.add(versionParam2);
      assertEquals(1, metas.size());
      assertEquals(new EntryVersionParam<>(new NumericEntryVersion(200)), metas.get(EntryVersionParam.getType()));

      EntryVersionParam<Long> versionParam3 = new EntryVersionParam<>(new NumericEntryVersion(300));
      EntryVersionParam<Long> versionParam4 = new EntryVersionParam<>(new NumericEntryVersion(400));
      metas.addMany(versionParam3, versionParam4);
      assertEquals(1, metas.size());
      assertEquals(new EntryVersionParam<>(new NumericEntryVersion(400)), metas.get(EntryVersionParam.getType()));
   }


}
