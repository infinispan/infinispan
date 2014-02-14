package org.infinispan.notifications.cachelistener;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntriesEvicted;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryActivated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryEvicted;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryLoaded;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryPassivated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryVisited;
import org.infinispan.notifications.cachelistener.event.CacheEntryActivatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvictedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryLoadedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryPassivatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryVisitedEvent;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;

/**
 * CustomClassLoaderListenerTest.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
@Test(groups = "functional", testName = "notifications.cachelistener.CustomClassLoaderListenerTest")
public class CustomClassLoaderListenerTest extends SingleCacheManagerTest {

   private CustomClassLoader ccl;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = getDefaultStandaloneCacheConfig(false);
      builder.persistence().passivation(true).addStore(DummyInMemoryStoreConfigurationBuilder.class);

      return TestCacheManagerFactory.createCacheManager(builder);
   }

   public void testCustomClassLoaderListener() throws Exception {
      ccl = new CustomClassLoader(Thread.currentThread().getContextClassLoader());
      ClassLoaderListener listener = new ClassLoaderListener();
      cache().getAdvancedCache().with(ccl).addListener(listener);

      cache().put("a", "a"); // Created + Modified
      assertEquals(1, listener.createdCounter);
      assertEquals(0, listener.modifiedCounter);
      assertEquals(0, listener.removedCounter);
      assertEquals(0, listener.visitedCounter);
      assertEquals(0, listener.activatedCounter);
      assertEquals(0, listener.passivatedCounter);
      assertEquals(0, listener.evictedCounter);
      assertEquals(0, listener.loadedCounter);
      listener.reset();

      cache().replace("a", "b"); // Modified
      assertEquals(0, listener.createdCounter);
      assertEquals(1, listener.modifiedCounter);
      assertEquals(0, listener.removedCounter);
      assertEquals(0, listener.visitedCounter);
      assertEquals(0, listener.activatedCounter);
      assertEquals(0, listener.passivatedCounter);
      assertEquals(0, listener.evictedCounter);
      assertEquals(0, listener.loadedCounter);
      listener.reset();

      cache().evict("a"); // Passivated + Evicted
      assertEquals(0, listener.createdCounter);
      assertEquals(0, listener.modifiedCounter);
      assertEquals(0, listener.removedCounter);
      assertEquals(0, listener.visitedCounter);
      assertEquals(0, listener.activatedCounter);
      assertEquals(1, listener.passivatedCounter);
      assertEquals(1, listener.evictedCounter);
      assertEquals(0, listener.loadedCounter);
      listener.reset();

      cache().get("a"); // Loaded + Activated + Visited
      assertEquals(0, listener.createdCounter);
      assertEquals(1, listener.modifiedCounter);
      assertEquals(0, listener.removedCounter);
      assertEquals(1, listener.visitedCounter);
      assertEquals(1, listener.activatedCounter);
      assertEquals(0, listener.passivatedCounter);
      assertEquals(0, listener.evictedCounter);
      assertEquals(1, listener.loadedCounter);
      listener.reset();

      cache().remove("a"); // Removed
      assertEquals(0, listener.createdCounter);
      assertEquals(0, listener.modifiedCounter);
      assertEquals(1, listener.removedCounter);
      assertEquals(0, listener.visitedCounter);
      assertEquals(0, listener.activatedCounter);
      assertEquals(0, listener.passivatedCounter);
      assertEquals(0, listener.evictedCounter);
      assertEquals(0, listener.loadedCounter);
   }

   public static class CustomClassLoader extends ClassLoader {
      public CustomClassLoader(ClassLoader parent) {
         super(parent);
      }
   }

   @Listener
   public class ClassLoaderListener {
      int createdCounter = 0;
      int removedCounter = 0;
      int modifiedCounter = 0;
      int visitedCounter = 0;
      int evictedCounter = 0;
      int passivatedCounter = 0;
      int loadedCounter = 0;
      int activatedCounter = 0;


      @CacheEntryCreated
      public void handleCreated(CacheEntryCreatedEvent e) {
         assertEquals(ccl, Thread.currentThread().getContextClassLoader());
         if (!e.isPre()) {
            createdCounter++;
         }
      }

      @CacheEntryRemoved
      public void handleRemoved(CacheEntryRemovedEvent e) {
         assertEquals(ccl, Thread.currentThread().getContextClassLoader());
         if (!e.isPre()) {
            removedCounter++;
         }
      }

      @CacheEntryModified
      public void handleModified(CacheEntryModifiedEvent e) {
         assertEquals(ccl, Thread.currentThread().getContextClassLoader());
         if (!e.isPre()) {
            modifiedCounter++;
         }
      }

      @CacheEntryVisited
      public void handleVisited(CacheEntryVisitedEvent e) {
         assertEquals(ccl, Thread.currentThread().getContextClassLoader());
         if (!e.isPre()) {
            visitedCounter++;
         }
      }

      @CacheEntryEvicted
      public void handleEvicted(CacheEntryEvictedEvent e) {
         assertEquals(ccl, Thread.currentThread().getContextClassLoader());
         if (!e.isPre()) {
            evictedCounter++;
         }
      }

      @CacheEntryPassivated
      public void handlePassivated(CacheEntryPassivatedEvent e) {
         assertEquals(ccl, Thread.currentThread().getContextClassLoader());
         if (!e.isPre()) {
            passivatedCounter++;
         }
      }

      @CacheEntryActivated
      public void handleActivated(CacheEntryActivatedEvent e) {
         assertEquals(ccl, Thread.currentThread().getContextClassLoader());
         if (!e.isPre()) {
            activatedCounter++;
         }
      }

      @CacheEntryLoaded
      public void handleLoaded(CacheEntryLoadedEvent e) {
         assertEquals(ccl, Thread.currentThread().getContextClassLoader());
         if (!e.isPre()) {
            loadedCounter++;
         }
      }

      void reset() {
         createdCounter = 0;
         removedCounter = 0;
         modifiedCounter = 0;
         visitedCounter = 0;
         evictedCounter = 0;
         passivatedCounter = 0;
         loadedCounter = 0;
         activatedCounter = 0;
      }
   }
}
