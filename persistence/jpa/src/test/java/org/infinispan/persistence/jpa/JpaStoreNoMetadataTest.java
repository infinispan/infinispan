package org.infinispan.persistence.jpa;

import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.marshall.persistence.impl.MarshalledEntryUtil;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.testng.annotations.Test;

/**
 * Test with metadata disabled
 */
@Test(groups = "unit", testName = "persistence.JpaStoreNoMetadataTest")
public class JpaStoreNoMetadataTest extends JpaStoreTest {
   @Override
   protected boolean storeMetadata() {
      return false;
   }

   @Override
   protected String getPersistenceUnitName() {
      return "org.infinispan.persistence.jpa.no_metadata";
   }

   @Test(enabled = false, description = "Not applicable")
   @Override
   public void testLoadAndStoreWithLifespan() throws Exception {
      // no metadata => cannot test lifespan
   }

   @Test(enabled = false, description = "Not applicable")
   @Override
   public void testLoadAndStoreWithIdle() throws Exception {
      // no metadata => cannot test idle
   }

   @Test(enabled = false, description = "Not applicable")
   @Override
   public void testLoadAndStoreWithLifespanAndIdle() throws Exception {
      // no metadata => cannot test lifespan or idle
   }

   @Test(enabled = false, description = "Not applicable")
   @Override
   public void testLoadAndStoreWithLifespanAndIdle2() throws Exception {
      // no metadata => cannot test lifespan or idle
   }


   @Test(enabled = false, description = "Not applicable")
   @Override
   public void testReplaceExpiredEntry() throws Exception {
      // no metadata => cannot test lifespan
   }

   // Without metadata we cannot purge anything - we should test this expected behaviour
   @Override
   public void testPurgeExpired() throws Exception {
      long lifespan = 6000;
      long idle = 4000;
      InternalCacheEntry ice1 = TestInternalCacheEntryFactory.create("k1", wrap("k1", "v1"), lifespan);
      cl.write(MarshalledEntryUtil.create(ice1, getMarshaller()));
      InternalCacheEntry ice2 = TestInternalCacheEntryFactory.create("k2", wrap("k2", "v2"), -1, idle);
      cl.write(MarshalledEntryUtil.create(ice2, getMarshaller()));
      InternalCacheEntry ice3 = TestInternalCacheEntryFactory.create("k3", wrap("k3", "v3"), lifespan, idle);
      cl.write(MarshalledEntryUtil.create(ice3, getMarshaller()));
      InternalCacheEntry ice4 = TestInternalCacheEntryFactory.create("k4", wrap("k4", "v4"), -1, -1);
      cl.write(MarshalledEntryUtil.create(ice4, getMarshaller()));
      InternalCacheEntry ice5 = TestInternalCacheEntryFactory.create("k5", wrap("k5", "v5"), lifespan * 1000, idle * 1000);
      cl.write(MarshalledEntryUtil.create(ice5, getMarshaller()));
      assertTrue(cl.contains("k1"));
      assertTrue(cl.contains("k2"));
      assertTrue(cl.contains("k3"));
      assertTrue(cl.contains("k4"));
      assertTrue(cl.contains("k5"));

      purgeExpired();

      assertTrue(cl.contains("k1"));
      assertTrue(cl.contains("k2"));
      assertTrue(cl.contains("k3"));
      assertTrue(cl.contains("k4"));
      assertTrue(cl.contains("k5"));
   }

   @Override
   public void testStopStartDoesNotNukeValues() throws InterruptedException, PersistenceException {
      // Without metadata the entries do not expire - we should test this expected behaviour
      assertFalse(cl.contains("k1"));
      assertFalse(cl.contains("k2"));

      long lifespan = 1;
      long idle = 1;
      InternalCacheEntry se1 = TestInternalCacheEntryFactory.create("k1", wrap("k1", "v1"), lifespan);
      InternalCacheEntry se2 = TestInternalCacheEntryFactory.create("k2", wrap("k2", "v2"));
      InternalCacheEntry se3 = TestInternalCacheEntryFactory.create("k3", wrap("k3", "v3"), -1, idle);
      InternalCacheEntry se4 = TestInternalCacheEntryFactory.create("k4", wrap("k4", "v4"), lifespan, idle);

      cl.write(MarshalledEntryUtil.create(se1, getMarshaller()));
      cl.write(MarshalledEntryUtil.create(se2, getMarshaller()));
      cl.write(MarshalledEntryUtil.create(se3, getMarshaller()));
      cl.write(MarshalledEntryUtil.create(se4, getMarshaller()));

      timeService.advance(lifespan + 1);

      cl.stop();
      cl.start();
      assertTrue(se1.isExpired(System.currentTimeMillis()));
      assertTrue(cl.contains("k1"));
      assertEquals("v1", unwrap(cl.loadEntry("k1").getValue()));
      assertTrue(cl.contains("k2"));
      assertEquals("v2", unwrap(cl.loadEntry("k2").getValue()));
      assertTrue(se3.isExpired(System.currentTimeMillis()));
      assertTrue(cl.contains("k3"));
      assertEquals("v3", unwrap(cl.loadEntry("k3").getValue()));
      assertTrue(se3.isExpired(System.currentTimeMillis()));
      assertTrue(cl.contains("k4"));
      assertEquals("v4", unwrap(cl.loadEntry("k4").getValue()));
   }
}
