package org.infinispan.notifications;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryActivated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryLoaded;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryPassivated;
import org.infinispan.notifications.cachelistener.event.CacheEntryPassivatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

@Test(groups = "functional", testName = "notifications.CacheListenerCacheLoaderTest")
public class CacheListenerCacheLoaderTest extends AbstractInfinispanTest {

   EmbeddedCacheManager cm;

   @BeforeMethod
   public void setUp() {
      cm = TestCacheManagerFactory.createCacheManager(false);
      ConfigurationBuilder c = new ConfigurationBuilder();
      c.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class)
         .storeName("no_passivation");
      cm.defineConfiguration("no_passivation", c.build());

      new ConfigurationBuilder();
      c.persistence().passivation(true).addStore(DummyInMemoryStoreConfigurationBuilder.class)
         .storeName("passivation");
      cm.defineConfiguration("passivation", c.build());
   }

   @AfterMethod
   public void tearDown() {
      TestingUtil.killCacheManagers(cm);
      cm = null;
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
   static public class TestListener {
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
      public void handlePassivated(CacheEntryPassivatedEvent e) {
         if (e.isPre()) passivated.add(e.getKey());
      }

      void reset() {
         loaded.clear();
         activated.clear();
         passivated.clear();
      }
   }
}