package org.infinispan.container;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.MortalCacheEntry;
import org.infinispan.container.entries.TransientCacheEntry;
import org.infinispan.container.entries.TransientMortalCacheEntry;
import org.infinispan.eviction.ActivationManager;
import org.infinispan.expiration.ExpirationManager;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.util.CoreImmutables;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "container.SimpleDataContainerTest")
public class SimpleDataContainerTest extends AbstractInfinispanTest {
   DataContainer<Object, String> dc;

   @BeforeMethod
   public void setUp() {
      dc = createContainer();
   }

   @AfterMethod
   public void tearDown() {
      dc = null;
   }

   protected DataContainer createContainer() {
      DefaultDataContainer dc = new DefaultDataContainer<Object, String>(16);
      InternalEntryFactoryImpl internalEntryFactory = new InternalEntryFactoryImpl();
      internalEntryFactory.injectTimeService(TIME_SERVICE);
      ActivationManager activationManager = mock(ActivationManager.class);
      doNothing().when(activationManager).onUpdate(Mockito.anyObject(), Mockito.anyBoolean());
      dc.initialize(null, null, internalEntryFactory, activationManager, null, TIME_SERVICE, null, mock(
              ExpirationManager.class));
      return dc;
   }

   public void testExpiredData() throws InterruptedException {
      dc.put("k", "v", new EmbeddedMetadata.Builder().maxIdle(100, TimeUnit.MINUTES).build());
      Thread.sleep(100);

      InternalCacheEntry entry = dc.get("k");
      assertNotNull(entry);
      assertEquals(transienttype(), entry.getClass());
      assertTrue(entry.getLastUsed() <= System.currentTimeMillis());
      long entryLastUsed = entry.getLastUsed();
      Thread.sleep(100);
      entry = dc.get("k");
      assertTrue(entry.getLastUsed() > entryLastUsed);
      dc.put("k", "v", new EmbeddedMetadata.Builder().maxIdle(0, TimeUnit.MINUTES).build());

      dc.put("k", "v", new EmbeddedMetadata.Builder().lifespan(100, TimeUnit.MINUTES).build());
      Thread.sleep(100);
      assertEquals(1, dc.size());

      entry = dc.get("k");
      assertNotNull(entry);
      assertEquals(mortaltype(), entry.getClass());
      assertTrue(entry.getCreated() <= System.currentTimeMillis());

      dc.put("k", "v", new EmbeddedMetadata.Builder().lifespan(0, TimeUnit.MINUTES).build());
      Thread.sleep(10);
      assertNull(dc.get("k"));
      assertEquals(0, dc.size());

      dc.put("k", "v", new EmbeddedMetadata.Builder().lifespan(0, TimeUnit.MINUTES).build());
      Thread.sleep(100);
      assertEquals(0, dc.size());
   }

   public void testResetOfCreationTime() throws Exception {
      long now = System.currentTimeMillis();
      dc.put("k", "v", new EmbeddedMetadata.Builder().lifespan(1000, TimeUnit.SECONDS).build());
      long created1 = dc.get("k").getCreated();
      assertTrue(created1 >= now);
      Thread.sleep(100);
      dc.put("k", "v", new EmbeddedMetadata.Builder().lifespan(1000, TimeUnit.SECONDS).build());
      long created2 = dc.get("k").getCreated();
      assertTrue("Expected " + created2 + " to be greater than " + created1, created2 > created1);
   }

   public void testUpdatingLastUsed() throws Exception {
      long idle = 600000;
      dc.put("k", "v", new EmbeddedMetadata.Builder().build());
      InternalCacheEntry ice = dc.get("k");
      assertEquals(immortaltype(), ice.getClass());
      assertEquals(-1, ice.toInternalCacheValue().getExpiryTime());
      assertEquals(-1, ice.getMaxIdle());
      assertEquals(-1, ice.getLifespan());
      dc.put("k", "v", new EmbeddedMetadata.Builder().maxIdle(idle, TimeUnit.MILLISECONDS).build());
      long oldTime = System.currentTimeMillis();
      Thread.sleep(100); // for time calc granularity
      ice = dc.get("k");
      assertEquals(transienttype(), ice.getClass());
      assertTrue(ice.toInternalCacheValue().getExpiryTime() > -1);
      assertTrue(ice.getLastUsed() > oldTime);
      Thread.sleep(100); // for time calc granularity
      assertTrue(ice.getLastUsed() < System.currentTimeMillis());
      assertEquals(idle, ice.getMaxIdle());
      assertEquals(-1, ice.getLifespan());

      oldTime = System.currentTimeMillis();
      Thread.sleep(100); // for time calc granularity
      assertNotNull(dc.get("k"));

      // check that the last used stamp has been updated on a get
      assertTrue(ice.getLastUsed() > oldTime);
      Thread.sleep(100); // for time calc granularity
      assertTrue(ice.getLastUsed() < System.currentTimeMillis());
   }

   protected Class<? extends InternalCacheEntry> mortaltype() {
      return MortalCacheEntry.class;
   }

   protected Class<? extends InternalCacheEntry> immortaltype() {
      return ImmortalCacheEntry.class;
   }

   protected Class<? extends InternalCacheEntry> transienttype() {
      return TransientCacheEntry.class;
   }

   protected Class<? extends InternalCacheEntry> transientmortaltype() {
      return TransientMortalCacheEntry.class;
   }


   public void testExpirableToImmortalAndBack() {
      String value = "v";
      dc.put("k", value, new EmbeddedMetadata.Builder().lifespan(100, TimeUnit.MINUTES).build());
      assertContainerEntry(mortaltype(), value);

      value = "v2";
      dc.put("k", value, new EmbeddedMetadata.Builder().build());
      assertContainerEntry(immortaltype(), value);

      value = "v3";
      dc.put("k", value, new EmbeddedMetadata.Builder().maxIdle(100, TimeUnit.MINUTES).build());
      assertContainerEntry(transienttype(), value);

      value = "v4";
      dc.put("k", value, new EmbeddedMetadata.Builder()
            .lifespan(100, TimeUnit.MINUTES).maxIdle(100, TimeUnit.MINUTES).build());
      assertContainerEntry(transientmortaltype(), value);

      value = "v41";
      dc.put("k", value, new EmbeddedMetadata.Builder()
            .lifespan(100, TimeUnit.MINUTES).maxIdle(100, TimeUnit.MINUTES).build());
      assertContainerEntry(transientmortaltype(), value);

      value = "v5";
      dc.put("k", value, new EmbeddedMetadata.Builder().lifespan(100, TimeUnit.MINUTES).build());
      assertContainerEntry(mortaltype(), value);
   }

   private void assertContainerEntry(Class<? extends InternalCacheEntry> type,
                                     String expectedValue) {
      assertTrue(dc.containsKey("k"));
      InternalCacheEntry entry = dc.get("k");
      assertEquals(type, entry.getClass());
      assertEquals(expectedValue, entry.getValue());
   }

   public void testKeySet() {
      dc.put("k1", "v", new EmbeddedMetadata.Builder().lifespan(100, TimeUnit.MINUTES).build());
      dc.put("k2", "v", new EmbeddedMetadata.Builder().build());
      dc.put("k3", "v", new EmbeddedMetadata.Builder().maxIdle(100, TimeUnit.MINUTES).build());
      dc.put("k4", "v", new EmbeddedMetadata.Builder()
            .maxIdle(100, TimeUnit.MINUTES).lifespan(100, TimeUnit.MINUTES).build());

      Set expected = new HashSet();
      expected.add("k1");
      expected.add("k2");
      expected.add("k3");
      expected.add("k4");

      for (Object o : dc.keySet()) assertTrue(expected.remove(o));

      assertTrue("Did not see keys " + expected + " in iterator!", expected.isEmpty());
   }

   public void testContainerIteration() {
      dc.put("k1", "v", new EmbeddedMetadata.Builder().lifespan(100, TimeUnit.MINUTES).build());
      dc.put("k2", "v", new EmbeddedMetadata.Builder().build());
      dc.put("k3", "v", new EmbeddedMetadata.Builder().maxIdle(100, TimeUnit.MINUTES).build());
      dc.put("k4", "v", new EmbeddedMetadata.Builder()
            .maxIdle(100, TimeUnit.MINUTES).lifespan(100, TimeUnit.MINUTES).build());

      Set expected = new HashSet();
      expected.add("k1");
      expected.add("k2");
      expected.add("k3");
      expected.add("k4");

      for (InternalCacheEntry ice : dc) {
         assertTrue(expected.remove(ice.getKey()));
      }

      assertTrue("Did not see keys " + expected + " in iterator!", expected.isEmpty());
   }

   public void testKeys() {
      dc.put("k1", "v1", new EmbeddedMetadata.Builder().lifespan(100, TimeUnit.MINUTES).build());
      dc.put("k2", "v2", new EmbeddedMetadata.Builder().build());
      dc.put("k3", "v3", new EmbeddedMetadata.Builder().maxIdle(100, TimeUnit.MINUTES).build());
      dc.put("k4", "v4", new EmbeddedMetadata.Builder()
            .maxIdle(100, TimeUnit.MINUTES).lifespan(100, TimeUnit.MINUTES).build());

      Set expected = new HashSet();
      expected.add("k1");
      expected.add("k2");
      expected.add("k3");
      expected.add("k4");

      for (Object o : dc.keySet()) assertTrue(expected.remove(o));

      assertTrue("Did not see keys " + expected + " in iterator!", expected.isEmpty());
   }

   public void testValues() {
      dc.put("k1", "v1", new EmbeddedMetadata.Builder().lifespan(100, TimeUnit.MINUTES).build());
      dc.put("k2", "v2", new EmbeddedMetadata.Builder().build());
      dc.put("k3", "v3", new EmbeddedMetadata.Builder().maxIdle(100, TimeUnit.MINUTES).build());
      dc.put("k4", "v4", new EmbeddedMetadata.Builder()
            .maxIdle(100, TimeUnit.MINUTES).lifespan(100, TimeUnit.MINUTES).build());

      Set expected = new HashSet();
      expected.add("v1");
      expected.add("v2");
      expected.add("v3");
      expected.add("v4");

      for (Object o : dc.values()) assertTrue(expected.remove(o));

      assertTrue("Did not see keys " + expected + " in iterator!", expected.isEmpty());
   }

   public void testEntrySet() {
      dc.put("k1", "v1", new EmbeddedMetadata.Builder().lifespan(100, TimeUnit.MINUTES).build());
      dc.put("k2", "v2", new EmbeddedMetadata.Builder().build());
      dc.put("k3", "v3", new EmbeddedMetadata.Builder().maxIdle(100, TimeUnit.MINUTES).build());
      dc.put("k4", "v4", new EmbeddedMetadata.Builder()
            .maxIdle(100, TimeUnit.MINUTES).lifespan(100, TimeUnit.MINUTES).build());

      Set<Map.Entry> expected = new HashSet<>();
      expected.add(CoreImmutables.immutableInternalCacheEntry(dc.get("k1")));
      expected.add(CoreImmutables.immutableInternalCacheEntry(dc.get("k2")));
      expected.add(CoreImmutables.immutableInternalCacheEntry(dc.get("k3")));
      expected.add(CoreImmutables.immutableInternalCacheEntry(dc.get("k4")));

      Set<Map.Entry> actual = new HashSet<>();
      for (Map.Entry o : dc.entrySet()) assertTrue(actual.add(o));

      assertEquals("Expected to see keys " + expected + " but only saw " + actual, expected, actual);
   }

   public void testGetDuringKeySetLoop() {
      for (int i = 0; i < 10; i++) dc.put(i, "value", new EmbeddedMetadata.Builder().build());

      int i = 0;
      for (Object key : dc.keySet()) {
         dc.peek(key); // calling get in this situations will result on corruption the iteration.
         i++;
      }

      assertEquals(10, i);
   }
}
