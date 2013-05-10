/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.container;

import org.infinispan.EmbeddedMetadata;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.MortalCacheEntry;
import org.infinispan.container.entries.TransientCacheEntry;
import org.infinispan.container.entries.TransientMortalCacheEntry;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.util.AnyEquivalence;
import org.infinispan.util.Immutables;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.testng.AssertJUnit.assertEquals;

@Test(groups = "unit", testName = "container.SimpleDataContainerTest")
public class SimpleDataContainerTest extends AbstractInfinispanTest {
   DataContainer dc;

   @BeforeMethod
   public void setUp() {
      dc = createContainer();
   }

   @AfterMethod
   public void tearDown() {
      dc = null;
   }

   protected DataContainer createContainer() {
      DefaultDataContainer dc = new DefaultDataContainer(16, AnyEquivalence.OBJECT, AnyEquivalence.OBJECT);
      dc.initialize(null, null, new InternalEntryFactoryImpl(), null, null);
      return dc;
   }

   public void testExpiredData() throws InterruptedException {
      dc.put("k", "v", new EmbeddedMetadata.Builder().maxIdle(100, TimeUnit.MINUTES).build());
      Thread.sleep(100);

      InternalCacheEntry entry = dc.get("k");
      assert entry.getClass().equals(transienttype());
      assert entry.getLastUsed() <= System.currentTimeMillis();
      long entryLastUsed = entry.getLastUsed();
      Thread.sleep(100);
      entry = dc.get("k");
      assert entry.getLastUsed() > entryLastUsed;
      dc.put("k", "v", new EmbeddedMetadata.Builder().maxIdle(0, TimeUnit.MINUTES).build());
      dc.purgeExpired();

      dc.put("k", "v", new EmbeddedMetadata.Builder().lifespan(100, TimeUnit.MINUTES).build());
      Thread.sleep(100);
      assert dc.size() == 1;

      entry = dc.get("k");
      assert entry != null : "Entry should not be null!";
      assert entry.getClass().equals(mortaltype()) : "Expected "+mortaltype()+", was " + entry.getClass().getSimpleName();
      assert entry.getCreated() <= System.currentTimeMillis();

      dc.put("k", "v", new EmbeddedMetadata.Builder().lifespan(0, TimeUnit.MINUTES).build());
      Thread.sleep(10);
      assert dc.get("k") == null;
      assert dc.size() == 0;

      dc.put("k", "v", new EmbeddedMetadata.Builder().lifespan(0, TimeUnit.MINUTES).build());
      Thread.sleep(100);
      assert dc.size() == 1;
      dc.purgeExpired();
      assert dc.size() == 0;
   }
   
   public void testResetOfCreationTime() throws Exception {
      long now = System.currentTimeMillis();
      dc.put("k", "v", new EmbeddedMetadata.Builder().lifespan(1000, TimeUnit.SECONDS).build());
      long created1 = dc.get("k").getCreated();
      assert created1 >= now;
      Thread.sleep(100);
      dc.put("k", "v", new EmbeddedMetadata.Builder().lifespan(1000, TimeUnit.SECONDS).build());
      long created2 = dc.get("k").getCreated();
      assert created2 > created1 : "Expected " + created2 + " to be greater than " + created1;
   }

   public void testUpdatingLastUsed() throws Exception {
      long idle = 600000;
      dc.put("k", "v", new EmbeddedMetadata.Builder().build());
      InternalCacheEntry ice = dc.get("k");
      assert ice.getClass().equals(immortaltype());
      assert ice.getExpiryTime() == -1;
      assert ice.getMaxIdle() == -1;
      assert ice.getLifespan() == -1;
      dc.put("k", "v", new EmbeddedMetadata.Builder().maxIdle(idle, TimeUnit.MILLISECONDS).build());
      long oldTime = System.currentTimeMillis();
      Thread.sleep(100); // for time calc granularity
      ice = dc.get("k");
      assert ice.getClass().equals(transienttype());
      assert ice.getExpiryTime() > -1;
      assert ice.getLastUsed() > oldTime;
      Thread.sleep(100); // for time calc granularity
      assert ice.getLastUsed() < System.currentTimeMillis();
      assert ice.getMaxIdle() == idle;
      assert ice.getLifespan() == -1;

      oldTime = System.currentTimeMillis();
      Thread.sleep(100); // for time calc granularity
      assert dc.get("k") != null;

      // check that the last used stamp has been updated on a get
      assert ice.getLastUsed() > oldTime;
      Thread.sleep(100); // for time calc granularity
      assert ice.getLastUsed() < System.currentTimeMillis();
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
      assert dc.containsKey("k");
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

      for (Object o : dc.keySet()) assert expected.remove(o);

      assert expected.isEmpty() : "Did not see keys " + expected + " in iterator!";
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
         assert expected.remove(ice.getKey());
      }

      assert expected.isEmpty() : "Did not see keys " + expected + " in iterator!";
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

      for (Object o : dc.keySet()) assert expected.remove(o);

      assert expected.isEmpty() : "Did not see keys " + expected + " in iterator!";
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

      for (Object o : dc.values()) assert expected.remove(o);

      assert expected.isEmpty() : "Did not see keys " + expected + " in iterator!";
   }

   public void testEntrySet() {
      dc.put("k1", "v1", new EmbeddedMetadata.Builder().lifespan(100, TimeUnit.MINUTES).build());
      dc.put("k2", "v2", new EmbeddedMetadata.Builder().build());
      dc.put("k3", "v3", new EmbeddedMetadata.Builder().maxIdle(100, TimeUnit.MINUTES).build());
      dc.put("k4", "v4", new EmbeddedMetadata.Builder()
            .maxIdle(100, TimeUnit.MINUTES).lifespan(100, TimeUnit.MINUTES).build());

      Set expected = new HashSet();
      expected.add(Immutables.immutableInternalCacheEntry(dc.get("k1")));
      expected.add(Immutables.immutableInternalCacheEntry(dc.get("k2")));
      expected.add(Immutables.immutableInternalCacheEntry(dc.get("k3")));
      expected.add(Immutables.immutableInternalCacheEntry(dc.get("k4")));

      Set actual = new HashSet();
      for (Map.Entry o : dc.entrySet()) actual.add(o);

      assert actual.equals(expected) : "Expected to see keys " + expected + " but only saw " + actual;
   }

   public void testGetDuringKeySetLoop() {
      for (int i = 0; i < 10; i++) dc.put(i, "value", new EmbeddedMetadata.Builder().build());

      int i = 0;
      for (Object key : dc.keySet()) {
         dc.peek(key); // calling get in this situations will result on corruption the iteration.
         i++;
      }

      assert i == 10 : "Expected the loop to run 10 times, only ran " + i;
   }   
}
