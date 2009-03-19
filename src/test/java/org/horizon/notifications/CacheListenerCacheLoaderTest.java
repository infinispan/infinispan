package org.horizon.notifications;

import org.horizon.Cache;
import org.horizon.config.CacheLoaderManagerConfig;
import org.horizon.config.Configuration;
import org.horizon.loader.dummy.DummyInMemoryCacheStore;
import org.horizon.manager.CacheManager;
import org.horizon.manager.DefaultCacheManager;
import org.horizon.notifications.cachelistener.annotation.CacheEntryActivated;
import org.horizon.notifications.cachelistener.annotation.CacheEntryLoaded;
import org.horizon.notifications.cachelistener.annotation.CacheEntryPassivated;
import org.horizon.notifications.cachelistener.event.CacheEntryEvent;
import org.horizon.test.TestingUtil;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

@Test(groups = "functional", testName = "notifications.CacheListenerCacheLoaderTest")
public class CacheListenerCacheLoaderTest {

   CacheManager cm;

   @BeforeMethod
   public void setUp() {
      cm = new DefaultCacheManager();
      Configuration c = new Configuration();
      CacheLoaderManagerConfig clmc = new CacheLoaderManagerConfig();
      DummyInMemoryCacheStore.Cfg clc = new DummyInMemoryCacheStore.Cfg("no_passivation");
      clmc.addCacheLoaderConfig(clc);
      c.setCacheLoaderManagerConfig(clmc);
      cm.defineCache("no_passivation", c);

      c = c.clone();
      ((DummyInMemoryCacheStore.Cfg) c.getCacheLoaderManagerConfig().getFirstCacheLoaderConfig()).setStore("passivation");
      c.getCacheLoaderManagerConfig().setPassivation(true);
      cm.defineCache("passivation", c);
   }

   @AfterMethod
   public void tearDown() {
      TestingUtil.killCacheManagers(cm);
   }

   public void testLoadingAndStoring() {
      Cache c = cm.getCache("no_passivation");
      TestListener l = new TestListener();
      c.addListener(l);

      assert l.loaded.isEmpty();
      assert l.activated.isEmpty();
      assert l.passivated.isEmpty();

      c.put("k", "v");

      assert l.loaded.isEmpty();
      assert l.activated.isEmpty();
      assert l.passivated.isEmpty();

      c.evict("k");

      assert l.loaded.isEmpty();
      assert l.activated.isEmpty();
      assert l.passivated.isEmpty();

      c.remove("k");

      assert l.loaded.contains("k");
      assert l.loaded.size() == 1;
      assert l.activated.isEmpty();
      assert l.passivated.isEmpty();

      c.put("k", "v");
      c.evict("k");

      assert l.loaded.size() == 1;
      assert l.activated.isEmpty();
      assert l.passivated.isEmpty();

      c.putAll(Collections.singletonMap("k2", "v2"));
      assert l.loaded.size() == 1;
      assert l.activated.isEmpty();
      assert l.passivated.isEmpty();

      c.putAll(Collections.singletonMap("k", "v-new"));
      assert l.passivated.isEmpty();
      assert l.loaded.size() == 1;
      assert l.activated.isEmpty();

      c.clear();
      assert l.passivated.isEmpty();
      assert l.loaded.size() == 1;
      assert l.activated.isEmpty();

      c.putAll(Collections.singletonMap("k2", "v-new"));
      c.evict("k2");
      assert l.passivated.isEmpty();
      assert l.loaded.size() == 1;
      assert l.activated.isEmpty();

      c.replace("k2", "something");
      assert l.passivated.isEmpty();
      assert l.loaded.size() == 2;
      assert l.loaded.contains("k2");
      assert l.activated.isEmpty();
   }

   public void testActivatingAndPassivating() {
      Cache c = cm.getCache("passivation");
      TestListener l = new TestListener();
      c.addListener(l);

      assert l.loaded.isEmpty();
      assert l.activated.isEmpty();
      assert l.passivated.isEmpty();

      c.put("k", "v");

      assert l.loaded.isEmpty();
      assert l.activated.isEmpty();
      assert l.passivated.isEmpty();

      c.evict("k");

      assert l.loaded.isEmpty();
      assert l.activated.isEmpty();
      assert l.passivated.contains("k");

      c.remove("k");

      assert l.loaded.contains("k");
      assert l.activated.contains("k");
      assert l.passivated.contains("k");
   }


   @Listener
   public class TestListener {
      List<Object> loaded = new LinkedList<Object>();
      List<Object> activated = new LinkedList<Object>();
      List<Object> passivated = new LinkedList<Object>();

      @CacheEntryLoaded
      public void handleLoaded(CacheEntryEvent e) {
         if (e.isPre()) loaded.add(e.getKey());
      }

      @CacheEntryActivated
      public void handleActivated(CacheEntryEvent e) {
         if (e.isPre()) activated.add(e.getKey());
      }

      @CacheEntryPassivated
      public void handlePassivated(CacheEntryEvent e) {
         if (e.isPre()) passivated.add(e.getKey());
      }

      void reset() {
         loaded.clear();
         activated.clear();
         passivated.clear();
      }
   }
}