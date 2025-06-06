package org.infinispan.container;

import static org.mockito.Mockito.mock;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.infinispan.commons.time.ControlledTimeService;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.MortalCacheEntry;
import org.infinispan.container.entries.TransientCacheEntry;
import org.infinispan.container.entries.TransientMortalCacheEntry;
import org.infinispan.container.impl.DefaultDataContainer;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.container.impl.InternalEntryFactoryImpl;
import org.infinispan.expiration.impl.InternalExpirationManager;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.CoreImmutables;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "container.SimpleDataContainerTest")
public class SimpleDataContainerTest extends AbstractInfinispanTest {
   private InternalDataContainer<String, String> dc;

   private ControlledTimeService timeService;

   @BeforeMethod
   public void setUp() {
      dc = createContainer();
   }

   @AfterMethod
   public void tearDown() {
      dc = null;
   }

   protected InternalDataContainer<String, String> createContainer() {
      DefaultDataContainer<String, String> dc = new DefaultDataContainer<>(16);
      InternalEntryFactoryImpl internalEntryFactory = new InternalEntryFactoryImpl();
      timeService = new ControlledTimeService();
      TestingUtil.inject(internalEntryFactory, timeService);
      InternalExpirationManager<String, String> expirationManager = mock(InternalExpirationManager.class);
      Mockito.when(expirationManager.entryExpiredInMemory(Mockito.any(), Mockito.anyLong(), Mockito.anyBoolean())).thenReturn(CompletableFutures.completedTrue());
      TestingUtil.inject(dc, internalEntryFactory, timeService, expirationManager);
      return dc;
   }

   public void testResetOfCreationTime() {
      long now = timeService.wallClockTime();
      timeService.advance(1);
      dc.put("k", "v", new EmbeddedMetadata.Builder().lifespan(1000, TimeUnit.SECONDS).build());
      long created1 = dc.peek("k").getCreated();
      assertEquals(now + 1, created1);
      timeService.advance(100);
      dc.put("k", "v", new EmbeddedMetadata.Builder().lifespan(1000, TimeUnit.SECONDS).build());
      long created2 = dc.peek("k").getCreated();
      assertEquals(now + 101, created2);
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
      assertTrue(dc.hasExpirable());

      value = "v2";
      dc.put("k", value, new EmbeddedMetadata.Builder().build());
      assertContainerEntry(immortaltype(), value);
      assertFalse(dc.hasExpirable());

      value = "v3";
      dc.put("k", value, new EmbeddedMetadata.Builder().maxIdle(100, TimeUnit.MINUTES).build());
      assertContainerEntry(transienttype(), value);
      assertTrue(dc.hasExpirable());

      value = "v4";
      dc.put("k", value, new EmbeddedMetadata.Builder()
            .lifespan(100, TimeUnit.MINUTES).maxIdle(50, TimeUnit.MINUTES).build());
      assertContainerEntry(transientmortaltype(), value);
      assertTrue(dc.hasExpirable());

      value = "v41";
      dc.put("k", value, new EmbeddedMetadata.Builder()
            .lifespan(100, TimeUnit.MINUTES).maxIdle(50, TimeUnit.MINUTES).build());
      assertContainerEntry(transientmortaltype(), value);
      assertTrue(dc.hasExpirable());

      value = "v5";
      dc.put("k", value, new EmbeddedMetadata.Builder().lifespan(100, TimeUnit.MINUTES).build());
      assertContainerEntry(mortaltype(), value);
      assertTrue(dc.hasExpirable());

      value = "v6";
      // Max idle time is higher than lifespan so it is ignored
      dc.put("k", value, new EmbeddedMetadata.Builder().maxIdle(100000, TimeUnit.MINUTES).lifespan(100, TimeUnit.MINUTES).build());
      assertContainerEntry(mortaltype(), value);
      assertTrue(dc.hasExpirable());

      value = "v61";
      // Note that max idle micro seconds is smaller than 100 minutes, so it will apply both
      dc.put("k", value, new EmbeddedMetadata.Builder().maxIdle(100000, TimeUnit.MICROSECONDS).lifespan(100, TimeUnit.MINUTES).build());
      assertContainerEntry(transientmortaltype(), value);
      assertTrue(dc.hasExpirable());
   }

   private void assertContainerEntry(Class<? extends InternalCacheEntry> type,
                                     String expectedValue) {
      assertTrue(dc.containsKey("k"));
      InternalCacheEntry<String, String> entry = dc.peek("k");
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

      for (InternalCacheEntry<String, String> entry : dc) {
         String o = entry.getKey();
         assertTrue(expected.remove(o));
      }

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

      for (InternalCacheEntry<String, String> entry : dc) {
         String o = entry.getKey();
         assertTrue(expected.remove(o));
      }

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

      for (InternalCacheEntry<String, String> entry : dc) {
         String o = entry.getValue();
         assertTrue(expected.remove(o));
      }

      assertTrue("Did not see keys " + expected + " in iterator!", expected.isEmpty());
   }

   public void testEntrySet() {
      dc.put("k1", "v1", new EmbeddedMetadata.Builder().lifespan(100, TimeUnit.MINUTES).build());
      dc.put("k2", "v2", new EmbeddedMetadata.Builder().build());
      dc.put("k3", "v3", new EmbeddedMetadata.Builder().maxIdle(100, TimeUnit.MINUTES).build());
      dc.put("k4", "v4", new EmbeddedMetadata.Builder()
            .maxIdle(100, TimeUnit.MINUTES).lifespan(100, TimeUnit.MINUTES).build());

      Set<Map.Entry<String, String>> expected = new HashSet<>();
      expected.add(CoreImmutables.immutableInternalCacheEntry(dc.peek("k1")));
      expected.add(CoreImmutables.immutableInternalCacheEntry(dc.peek("k2")));
      expected.add(CoreImmutables.immutableInternalCacheEntry(dc.peek("k3")));
      expected.add(CoreImmutables.immutableInternalCacheEntry(dc.peek("k4")));

      Set<Map.Entry<String, String>> actual = new HashSet<>();
      for (Map.Entry<String, String> o : dc) {
         assertTrue(actual.add(o));
      }

      assertEquals("Expected to see keys " + expected + " but only saw " + actual, expected, actual);
   }

   public void testGetDuringKeySetLoop() {
      for (int i = 0; i < 10; i++) dc.put(String.valueOf(i), "value", new EmbeddedMetadata.Builder().build());

      int i = 0;
      for (InternalCacheEntry<String, String> entry : dc) {
         Object key = entry.getKey();
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
      Map.Entry<String, String> k1 = CoreImmutables.immutableInternalCacheEntry(dc.peek("k1"));
      expected.add(k1);
      expected.add(CoreImmutables.immutableInternalCacheEntry(dc.peek("k2")));
      Map.Entry<String, String> k3 = CoreImmutables.immutableInternalCacheEntry(dc.peek("k3"));
      expected.add(k3);

      List<Map.Entry<String, String>> results = StreamSupport.stream(dc.spliterator(), false).collect(Collectors.toList());
      assertEquals(3, results.size());
      assertArrayAndSetContainSame(expected, results);

      timeService.advance(101);

      results = StreamSupport.stream(dc.spliterator(), false).collect(Collectors.toList());
      assertEquals(2, results.size());
      expected.remove(k1);
      assertArrayAndSetContainSame(expected, results);

      timeService.advance(100);

      results = StreamSupport.stream(dc.spliterator(), false).collect(Collectors.toList());
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
