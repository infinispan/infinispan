package org.infinispan.server.xsite;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.commons.configuration.XMLStringConfiguration;
import org.infinispan.server.functional.XSiteIT;
import org.infinispan.server.test.junit4.InfinispanXSiteServerRule;
import org.infinispan.server.test.junit4.InfinispanXSiteServerTestMethodRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class XSiteExample {

   // Java arrays inherit .hashcode() and .equals() from Object. Because of that, we are using a List
   private static Map<List<Boolean>, Integer> expectedValueTable = new HashMap<>();

   @ClassRule
   public static final InfinispanXSiteServerRule SERVERS = XSiteIT.SERVERS;

   @Rule
   public InfinispanXSiteServerTestMethodRule SERVER_TEST = new InfinispanXSiteServerTestMethodRule(SERVERS);

   private boolean offHeap;
   private boolean maxIdle;
   private boolean store;
   private boolean eviction;

   public XSiteExample(boolean offHeap, boolean maxIdle, boolean store, boolean eviction) {
      this.offHeap = offHeap;
      this.maxIdle = maxIdle;
      this.store = store;
      this.eviction = eviction;
   }

   @Parameterized.Parameters(name = "offHeap={0}, maxIdle={1}, store={2}, eviction={3}")
   public static Collection<Boolean[]> data() {
      List<Boolean[]> params = new ArrayList<>();
      boolean[] configs = {true, false};
      for (boolean offHeap : configs) {
         for (boolean maxIdle : configs) {
            for (boolean store : configs) {
               for (boolean eviction : configs) {
                  if (offHeap && eviction) {
                     continue;
                  }
                  params.add(new Boolean[]{offHeap, maxIdle, store, eviction});

                  Integer expectedValue = 1000;
                  if (eviction) {
                     expectedValue = 500;
                  }
                  if (offHeap) {
                     expectedValue = 10;
                  }
                  // custom
                  if (offHeap && store) {
                     expectedValue = 1000;
                  }
                  if (eviction && store) {
                     expectedValue = 1000;
                  }
                  // last
                  if (maxIdle) {
                     expectedValue = 0;
                  }
                  expectedValueTable.put(Arrays.asList(offHeap, maxIdle, store, eviction), expectedValue);
               }
            }
         }
      }
      return params;
   }

   @Test
   public void testHotRodOperations() {

      RemoteCache<String, String> lonCache = createCacheConfig(offHeap, maxIdle, store, eviction, "LON", "NYC");
      RemoteCache<String, String> nycCache = createCacheConfig(offHeap, maxIdle, store, eviction, "NYC", "LON");

      List<Boolean> key = Arrays.asList(offHeap, maxIdle, store, eviction);
      Integer expectedValue = expectedValueTable.get(key);
      insertAndVerifyEntries(lonCache, nycCache, expectedValue);
   }

   private void insertAndVerifyEntries(RemoteCache<String, String> lonCache, RemoteCache<String, String> nycCache, Integer expectedValue) {
      for (int i = 0; i < 1_000; i++) {
         lonCache.put("key-" + i, "value-" + i);
      }

      // there is a reaper that runs
      if (maxIdle) {
         sleep(70_000);
      } else {
         sleep(5_000); // sleep isn't a bad thing, right ?
      }

      assertEquals(expectedValue, (Integer) nycCache.size());
   }

   private static void sleep(long timeMs) {
      try {
         Thread.sleep(timeMs);
      } catch (InterruptedException e) {
         throw new IllegalStateException(e);
      }
   }

   /**
    * off_heap = on/off
    * max-idle = on/off
    * store = on/off
    * eviction = on/off
    *
    * @return
    */
   private RemoteCache<String, String> createCacheConfig(boolean offHeap, boolean maxIdle, boolean store, boolean eviction, String where, String backup) {
      String configXml =
            String.format(
                  "<infinispan><cache-container>" +
                  "  <replicated-cache name=\"%s\">" +
                  "    <backups>" +
                  "      <backup site=\"%s\" strategy=\"SYNC\"/>" +
                  "    </backups>" +
                       offHeapXmlConfig(offHeap) +
                       maxIdleXmlConfig(maxIdle) +
                       storeXmlConfig(store) +
                       evictionXmlConfig(eviction) +
                  "  </replicated-cache>" +
                  "</cache-container></infinispan>", SERVER_TEST.getMethodName(), backup);
      return SERVER_TEST.hotrod(where)
            .withServerConfiguration(new XMLStringConfiguration(configXml)).create();
   }

   private String evictionXmlConfig(boolean eviction) {
      if (eviction) {
         return "<memory max-count=\"500\" when-full=\"REMOVE\"/>";
      } else {
         return  "";
      }
   }

   private String storeXmlConfig(boolean store) {
      String xml;
      if (store) {
         xml =
         "<persistence passivation=\"true\">" +
         "  <file-store max-entries=\"1000000\" shared=\"false\" preload=\"false\" fetch-state=\"true\" purge=\"true\"/>" +
         "</persistence>";
      } else {
         xml = "";
      }
      return xml;
   }

   private String maxIdleXmlConfig(boolean maxIdle) {
      if (maxIdle) {
         return "<expiration max-idle=\"1000\"/>";
      } else {
         return  "";
      }
   }

   private String offHeapXmlConfig(boolean offHeap) {
      if (offHeap) {
         return "<memory storage=\"OFF_HEAP\" max-count=\"10\" when-full=\"REMOVE\"/>";
      } else {
         return  "";
      }
   }
}
