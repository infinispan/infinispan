package org.infinispan.functional.impl;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Optional;

import org.infinispan.commons.api.functional.EntryVersion.NumericEntryVersion;
import org.infinispan.commons.api.functional.MetaParam.MetaCreated;
import org.infinispan.commons.api.functional.MetaParam.MetaEntryVersion;
import org.infinispan.commons.api.functional.MetaParam.MetaLastUsed;
import org.infinispan.commons.api.functional.MetaParam.MetaLifespan;
import org.infinispan.commons.api.functional.MetaParam.MetaMaxIdle;
import org.testng.annotations.Test;

/**
 * Unit test for metadata parameters collection.
 */
@Test(groups = "functional", testName = "functional.impl.MetaParamsTest")
public class MetaParamsTest {

   public void testEmptyMetaParamsFind() {
      MetaParams metas = MetaParams.empty();
      assertTrue(metas.isEmpty());
      assertEquals(0, metas.size());
      assertFalse(metas.find(MetaLifespan.class).isPresent());
      assertFalse(metas.find(MetaEntryVersion.class).isPresent());
      assertFalse(metas.find(MetaMaxIdle.class).isPresent());
   }

   @Test
   public void testAddFindMetaParam() {
      MetaParams metas = MetaParams.empty();
      MetaLifespan lifespan = new MetaLifespan(1000);
      metas.add(lifespan);
      assertFalse(metas.isEmpty());
      assertEquals(1, metas.size());
      Optional<MetaLifespan> lifespanFound = metas.find(MetaLifespan.class);
      assertEquals(new MetaLifespan(1000), lifespanFound.get());
      assertEquals(1000, metas.find(MetaLifespan.class).get().get().longValue());
      assertFalse(new MetaLifespan(900).equals(lifespanFound.get()));
      metas.add(new MetaLifespan(900));
      assertFalse(metas.isEmpty());
      assertEquals(1, metas.size());
      assertEquals(Optional.of(new MetaLifespan(900)), metas.find(lifespan.getClass()));
   }

   @Test
   public void testAddFindMultipleMetaParams() {
      MetaParams metas = MetaParams.empty();
      metas.addMany(new MetaLifespan(1000), new MetaMaxIdle(1000), new MetaEntryVersion<>(new NumericEntryVersion(12345)));
      assertFalse(metas.isEmpty());
      assertEquals(3, metas.size());
      Optional<MetaMaxIdle> maxIdle = metas.find(MetaMaxIdle.class);
      Class<MetaEntryVersion<Long>> type = MetaEntryVersion.type();
      Optional<MetaEntryVersion<Long>> entryVersion = metas.find(type);
      assertEquals(Optional.of(new MetaMaxIdle(1000)), maxIdle);
      assertFalse(900 == maxIdle.get().get().longValue());
      assertEquals(new MetaEntryVersion<>(new NumericEntryVersion(12345)), entryVersion.get());
      assertFalse(new MetaEntryVersion<>(new NumericEntryVersion(23456)).equals(entryVersion.get()));
   }

   @Test
   public void testReplaceFindMultipleMetaParams() {
      MetaParams metas = MetaParams.empty();
      metas.addMany(new MetaLifespan(1000), new MetaMaxIdle(1000), new MetaEntryVersion<>(new NumericEntryVersion(12345)));
      assertFalse(metas.isEmpty());
      assertEquals(3, metas.size());
      metas.addMany(new MetaLifespan(2000), new MetaMaxIdle(2000));
      assertFalse(metas.isEmpty());
      assertEquals(3, metas.size());
      assertEquals(Optional.of(new MetaMaxIdle(2000)), metas.find(MetaMaxIdle.class));
      assertEquals(Optional.of(new MetaLifespan(2000)), metas.find(MetaLifespan.class));
      assertEquals(Optional.of(new MetaEntryVersion<>(new NumericEntryVersion(12345))),
         metas.find(MetaEntryVersion.class));
   }

   @Test
   public void testConstructors() {
      MetaParams metasOf1 = MetaParams.of(new MetaCreated(1000));
      assertFalse(metasOf1.isEmpty());
      assertEquals(1, metasOf1.size());
      MetaParams metasOf2 = MetaParams.of(new MetaCreated(1000), new MetaLastUsed(2000));
      assertFalse(metasOf2.isEmpty());
      assertEquals(2, metasOf2.size());
      MetaParams metasOf4 = MetaParams.of(
         new MetaCreated(1000), new MetaLastUsed(2000), new MetaLifespan(3000), new MetaMaxIdle(4000));
      assertFalse(metasOf4.isEmpty());
      assertEquals(4, metasOf4.size());
   }

   @Test
   public void testDuplicateParametersOnConstruction() {
      MetaEntryVersion<Long> versionParam1 = new MetaEntryVersion<>(new NumericEntryVersion(100));
      MetaEntryVersion<Long> versionParam2 = new MetaEntryVersion<>(new NumericEntryVersion(200));
      MetaParams metas = MetaParams.of(versionParam1, versionParam2);
      assertEquals(1, metas.size());
      assertEquals(Optional.of(new MetaEntryVersion<>(new NumericEntryVersion(200))),
         metas.find(MetaEntryVersion.class));
   }

   @Test
   public void testDuplicateParametersOnAdd() {
      MetaEntryVersion<Long> versionParam1 = new MetaEntryVersion<>(new NumericEntryVersion(100));
      MetaParams metas = MetaParams.of(versionParam1);
      assertEquals(1, metas.size());
      assertEquals(Optional.of(new MetaEntryVersion<>(new NumericEntryVersion(100))),
         metas.find(MetaEntryVersion.class));

      MetaEntryVersion<Long> versionParam2 = new MetaEntryVersion<>(new NumericEntryVersion(200));
      metas.add(versionParam2);
      assertEquals(1, metas.size());
      assertEquals(Optional.of(new MetaEntryVersion<>(new NumericEntryVersion(200))),
         metas.find(MetaEntryVersion.class));

      MetaEntryVersion<Long> versionParam3 = new MetaEntryVersion<>(new NumericEntryVersion(300));
      MetaEntryVersion<Long> versionParam4 = new MetaEntryVersion<>(new NumericEntryVersion(400));
      metas.addMany(versionParam3, versionParam4);
      assertEquals(1, metas.size());
      assertEquals(Optional.of(new MetaEntryVersion<>(new NumericEntryVersion(400))),
         metas.find(MetaEntryVersion.class));
   }


}
