package org.infinispan.container;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.MortalCacheEntry;
import org.infinispan.container.entries.TransientCacheEntry;
import org.infinispan.container.entries.TransientMortalCacheEntry;
import org.infinispan.container.impl.DefaultDataContainer;
import org.infinispan.container.impl.InternalEntryFactoryImpl;
import org.infinispan.eviction.ActivationManager;
import org.infinispan.expiration.ExpirationManager;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.ControlledTimeService;
import org.infinispan.util.CoreImmutables;
import org.infinispan.util.concurrent.CompletableFutures;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "container.SimpleDataContainerTest")
public class SimpleDataContainerTest extends AbstractInfinispanTest {
   private DataContainer<String, String> dc;

   private ControlledTimeService timeService;

   @BeforeMethod
   public void setUp() {
      dc = createContainer();
   }

   @AfterMethod
   public void tearDown() {
      dc = null;
   }

   protected DataContainer<String, String> createContainer() {
      DefaultDataContainer<String, String> dc = new DefaultDataContainer<>(16);
      InternalEntryFactoryImpl internalEntryFactory = new InternalEntryFactoryImpl();
      timeService = new ControlledTimeService();
      TestingUtil.inject(internalEntryFactory, timeService);
      ActivationManager activationManager = mock(ActivationManager.class);
      doNothing().when(activationManager).onUpdate(Mockito.any(), Mockito.anyBoolean());
      ExpirationManager expirationManager = mock(ExpirationManager.class);
      Mockito.when(expirationManager.entryExpiredInMemory(Mockito.any(), Mockito.anyLong())).thenReturn(CompletableFutures.completedTrue());
      Mockito.when(expirationManager.entryExpiredInMemoryFromIteration(Mockito.any(), Mockito.anyLong())).thenReturn(CompletableFutures.completedTrue());
      TestingUtil.inject(dc, internalEntryFactory, activationManager, timeService, expirationManager);
      return dc;
   }

   public void testExpiredData() throws InterruptedException {
      dc.put("k", "v", new EmbeddedMetadata.Builder().maxIdle(100, TimeUnit.MINUTES).build());
      timeService.advance(100);

      InternalCacheEntry entry = dc.get("k");
      assertNotNull(entry);
      assertEquals(transienttype(), entry.getClass());
      assertEquals(timeService.wallClockTime(), entry.getLastUsed());
      long entryLastUsed = entry.getLastUsed();
      timeService.advance(100);
      entry = dc.get("k");
      assertEquals(entryLastUsed + 100, entry.getLastUsed());
      dc.put("k", "v", new EmbeddedMetadata.Builder().maxIdle(1, TimeUnit.MILLISECONDS).build());

      long oldTime = timeService.wallClockTime();
      dc.put("k", "v", new EmbeddedMetadata.Builder().lifespan(100, TimeUnit.MINUTES).build());
      timeService.advance(100);
      assertEquals(1, dc.size());

      entry = dc.get("k");
      assertNotNull(entry);
      assertEquals(mortaltype(), entry.getClass());
      assertEquals(oldTime, entry.getCreated());
      assertEquals(-1, entry.getMaxIdle());

      dc.put("k", "v", new EmbeddedMetadata.Builder().lifespan(1, TimeUnit.MILLISECONDS).build());
      timeService.advance(10);
      assertNull(dc.get("k"));
      assertEquals(0, dc.size());

      dc.put("k", "v", new EmbeddedMetadata.Builder().lifespan(1, TimeUnit.MILLISECONDS).build());
      timeService.advance(100);
      assertEquals(0, dc.size());
   }

   public void testResetOfCreationTime() throws Exception {
      long now = timeService.wallClockTime();
      timeService.advance(1);
      dc.put("k", "v", new EmbeddedMetadata.Builder().lifespan(1000, TimeUnit.SECONDS).build());
      long created1 = dc.get("k").getCreated();
      assertEquals(now + 1, created1);
      timeService.advance(100);
      dc.put("k", "v", new EmbeddedMetadata.Builder().lifespan(1000, TimeUnit.SECONDS).build());
      long created2 = dc.get("k").getCreated();
      assertEquals(now + 101, created2);
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
      timeService.advance(100); // for time calc granularity
      ice = dc.get("k");
      assertEquals(transienttype(), ice.getClass());
      assertEquals(idle + timeService.wallClockTime(), ice.toInternalCacheValue().getExpiryTime());
      assertEquals(timeService.wallClockTime(), ice.getLastUsed());
      assertEquals(idle, ice.getMaxIdle());
      assertEquals(-1, ice.getLifespan());

      timeService.advance(100); // for time calc granularity
      assertNotNull(dc.get("k"));

      long oldTime = timeService.wallClockTime();
      // check that the last used stamp has been updated on a get
      assertEquals(oldTime, ice.getLastUsed());

      timeService.advance(100); // for time calc granularity
      assertEquals(oldTime, ice.getLastUsed());
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

      Set<String> expected = new HashSet<>();
      expected.add("k1");
      expected.add("k2");
      expected.add("k3");
      expected.add("k4");

      for (String o : dc.keySet()) assertTrue(expected.remove(o));

      assertTrue("Did not see keys " + expected + " in iterator!", expected.isEmpty());
   }

   public void testContainerIteration() {
      dc.put("k1", "v", new EmbeddedMetadata.Builder().lifespan(100, TimeUnit.MINUTES).build());
      dc.put("k2", "v", new EmbeddedMetadata.Builder().build());
      dc.put("k3", "v", new EmbeddedMetadata.Builder().maxIdle(100, TimeUnit.MINUTES).build());
      dc.put("k4", "v", new EmbeddedMetadata.Builder()
            .maxIdle(100, TimeUnit.MINUTES).lifespan(100, TimeUnit.MINUTES).build());

      Set<String> expected = new HashSet<>();
      expected.add("k1");
      expected.add("k2");
      expected.add("k3");
      expected.add("k4");

      for (InternalCacheEntry<String, String> ice : dc) {
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

      Set<String> expected = new HashSet<>();
      expected.add("k1");
      expected.add("k2");
      expected.add("k3");
      expected.add("k4");

      for (String o : dc.keySet()) assertTrue(expected.remove(o));

      assertTrue("Did not see keys " + expected + " in iterator!", expected.isEmpty());
   }

   public void testValues() {
      dc.put("k1", "v1", new EmbeddedMetadata.Builder().lifespan(100, TimeUnit.MINUTES).build());
      dc.put("k2", "v2", new EmbeddedMetadata.Builder().build());
      dc.put("k3", "v3", new EmbeddedMetadata.Builder().maxIdle(100, TimeUnit.MINUTES).build());
      dc.put("k4", "v4", new EmbeddedMetadata.Builder()
            .maxIdle(100, TimeUnit.MINUTES).lifespan(100, TimeUnit.MINUTES).build());

      Set<String> expected = new HashSet<>();
      expected.add("v1");
      expected.add("v2");
      expected.add("v3");
      expected.add("v4");

      for (String o : dc.values()) assertTrue(expected.remove(o));

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
      for (Map.Entry<String, String> o : dc.entrySet()) assertTrue(actual.add(o));

      assertEquals("Expected to see keys " + expected + " but only saw " + actual, expected, actual);
   }

   public void testGetDuringKeySetLoop() {
      for (int i = 0; i < 10; i++) dc.put(String.valueOf(i), "value", new EmbeddedMetadata.Builder().build());

      int i = 0;
      for (Object key : dc.keySet()) {
         dc.peek(key); // calling get in this situations will result on corruption the iteration.
         i++;
      }

      assertEquals(10, i);
   }

   public void testEntrySetStreamWithExpiredEntries() {
      dc.put("k1", "v1", new EmbeddedMetadata.Builder().lifespan(100, TimeUnit.MILLISECONDS).build());
      dc.put("k2", "v2", new EmbeddedMetadata.Builder().build());
      dc.put("k3", "v3", new EmbeddedMetadata.Builder().maxIdle(200, TimeUnit.MILLISECONDS).build());

      Set<Map.Entry<String, String>> expected = new HashSet<>();
      Map.Entry<String, String> k1 = CoreImmutables.immutableInternalCacheEntry(dc.get("k1"));
      expected.add(k1);
      expected.add(CoreImmutables.immutableInternalCacheEntry(dc.get("k2")));
      Map.Entry<String, String> k3 = CoreImmutables.immutableInternalCacheEntry(dc.get("k3"));
      expected.add(k3);

      List<Map.Entry<String, String>> results = Arrays.asList(dc.entrySet().stream().toArray(Map.Entry[]::new));
      assertEquals(3, results.size());
      assertArrayAndSetContainSame(expected, results);

      timeService.advance(101);

      results = Arrays.asList(dc.entrySet().stream().toArray(Map.Entry[]::new));
      assertEquals(2, results.size());
      expected.remove(k1);
      assertArrayAndSetContainSame(expected, results);

      timeService.advance(100);

      results = Arrays.asList(dc.entrySet().stream().toArray(Map.Entry[]::new));
      assertEquals(1, results.size());
      expected.remove(k3);
      assertArrayAndSetContainSame(expected, results);
   }

   private <E> void assertArrayAndSetContainSame(Set<E> expected, List<E> results) {
      for (E result : results) {
         assertTrue("Set didn't contain " + result, expected.contains(result));
      }
   }
}
