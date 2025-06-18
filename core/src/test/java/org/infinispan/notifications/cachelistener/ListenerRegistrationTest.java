package org.infinispan.notifications.cachelistener;

import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.util.List;

import org.infinispan.AdvancedCache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.container.impl.InternalEntryFactoryImpl;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.notifications.IncorrectListenerException;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryVisited;
import org.infinispan.notifications.cachelistener.cluster.ClusterEventManager;
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "notifications.cachelistener.ListenerRegistrationTest")
public class ListenerRegistrationTest extends AbstractInfinispanTest {

   private CacheNotifierImpl newNotifier() {
      CacheNotifierImpl notifier = new CacheNotifierImpl();
      AdvancedCache mockCache = mock(AdvancedCache.class, RETURNS_DEEP_STUBS);
      ComponentRegistry registry = mock(ComponentRegistry.class);
      Configuration config = mock(Configuration.class, RETURNS_DEEP_STUBS);
      when(config.clustering().cacheMode()).thenReturn(CacheMode.LOCAL);
      when(config.memory().storage()).thenReturn(StorageType.HEAP);
      TestingUtil.inject(notifier, mockCache, new ClusteringDependentLogic.LocalLogic(), config,
                         new InternalEntryFactoryImpl(), mock(ClusterEventManager.class),
                         mock(BasicComponentRegistry.class));
      return notifier;
   }

   public void testControl() {
      Object l = new TestControlListener();
      CacheNotifierImpl n = newNotifier();
      n.addListener(l);
      assertEquals(1, n.getListeners().size());
   }

   public void testCacheListenerNoMethods() {
      Object l = new TestCacheListenerNoMethodsListener();
      CacheNotifierImpl n = newNotifier();
      n.addListener(l);
      assertEquals("Hello", l.toString());
      assertTrue("No listeners should be registered.", n.getListeners().isEmpty()); // since the valid listener has no methods to listen
   }

   public void testNonAnnotatedListener() {
      Object l = new TestNonAnnotatedListener();
      CacheNotifierImpl n = newNotifier();
      try {
         n.addListener(l);
         fail("Should not accept an un-annotated cache listener");
      } catch (IncorrectListenerException icle) {
         // expected
      }
      assertTrue("No listeners should be registered.", n.getListeners().isEmpty());
   }

   public void testNonPublicListener() {
      Object l = new TestNonPublicListener();
      CacheNotifierImpl n = newNotifier();
      try {
         n.addListener(l);
         fail("Should not accept a private callback class");
      } catch (IncorrectListenerException icle) {
         // expected
      }
      assertTrue("No listeners should be registered.", n.getListeners().isEmpty());
   }

   public void testNonPublicListenerMethod() {
      Object l = new TestNonPublicListenerMethodListener();
      CacheNotifierImpl n = newNotifier();
      n.addListener(l);

      // should not fail, should just not register anything

      assertTrue("No listeners should be registered.", n.getListeners().isEmpty());
   }

   public void testNonVoidReturnTypeMethod() {
      Object l = new TestNonVoidReturnTypeMethodListener();
      CacheNotifierImpl n = newNotifier();
      try {
         n.addListener(l);
         fail("Should not accept a listener method with a return type");
      } catch (IncorrectListenerException icle) {
         // expected
      }
      assertTrue("No listeners should be registered.", n.getListeners().isEmpty());
   }

   public void testIncorrectMethodSignature1() {
      Object l = new TestIncorrectMethodSignature1Listener();
      CacheNotifierImpl n = newNotifier();
      try {
         n.addListener(l);
         fail("Should not accept a cache listener with a bad method signature");
      } catch (IncorrectListenerException icle) {
         // expected
      }
      assertTrue("No listeners should be registered.", n.getListeners().isEmpty());
   }

   public void testIncorrectMethodSignature2() {
      Object l = new TestIncorrectMethodSignature2Listener();
      CacheNotifierImpl n = newNotifier();
      try {
         n.addListener(l);
         fail("Should not accept a cache listener with a bad method signature");
      } catch (IncorrectListenerException icle) {
         // expected
      }
      assertTrue("No listeners should be registered.", n.getListeners().isEmpty());
   }

   public void testIncorrectMethodSignature3() {
      Object l = new TestIncorrectMethodSignature3Listener();
      CacheNotifierImpl n = newNotifier();
      try {
         n.addListener(l);
         fail("Should not accept a cache listener with a bad method signature");
      } catch (IncorrectListenerException icle) {
         // expected
      }
      assertTrue("No listeners should be registered.", n.getListeners().isEmpty());
   }

   public void testUnassignableMethodSignature() {
      Object l = new TestUnassignableMethodSignatureListener();
      CacheNotifierImpl n = newNotifier();
      try {
         n.addListener(l);
         fail("Should not accept a cache listener with a bad method signature");
      } catch (IncorrectListenerException icle) {
         // expected
      }
      assertTrue("No listeners should be registered.", n.getListeners().isEmpty());
   }

   public void testPartlyUnassignableMethodSignature() {
      Object l = new TestPartlyUnassignableMethodSignatureListener();
      CacheNotifierImpl n = newNotifier();
      try {
         n.addListener(l);
         fail("Should not accept a cache listener with a bad method signature");
      } catch (IncorrectListenerException icle) {
         // expected
      }
   }

   public void testMultipleMethods() {
      Object l = new TestMultipleMethodsListener();
      CacheNotifierImpl n = newNotifier();
      n.addListener(l);
      List invocations = n.cacheEntryVisitedListeners;
      assertEquals(1, invocations.size());
      invocations = n.cacheEntryRemovedListeners;
      assertEquals(1, invocations.size());
      assertEquals(1, n.getListeners().size());
   }

   public void testMultipleAnnotationsOneMethod() {
      Object l = new TestMultipleAnnotationsOneMethodListener();
      CacheNotifierImpl n = newNotifier();
      n.addListener(l);
      List invocations = n.cacheEntryVisitedListeners;
      assertEquals(1, invocations.size());
      invocations = n.cacheEntryRemovedListeners;
      assertEquals(1, invocations.size());
      assertEquals(1, n.getListeners().size());
   }

   public void testMultipleMethodsOneAnnotation() {
      Object l = new TestMultipleMethodsOneAnnotationListener();
      CacheNotifierImpl n = newNotifier();
      n.addListener(l);
      List invocations = n.cacheEntryVisitedListeners;
      assertEquals(2, invocations.size());
      assertEquals(1, n.getListeners().size());
   }

   @Listener
   public static class TestControlListener {
      @CacheEntryVisited
      @CacheEntryRemoved
      public void callback(Event e) {
      }
   }

   @Listener
   public static class TestCacheListenerNoMethodsListener {
      public String toString() {
         return "Hello";
      }
   }

   public static class TestNonAnnotatedListener {
      public String toString() {
         return "Hello";
      }
   }

   @Listener
   protected static class TestNonPublicListener {
      @CacheEntryVisited
      public void callback() {
      }
   }

   @Listener
   public static class TestNonPublicListenerMethodListener {
      @CacheEntryVisited
      protected void callback(Event e) {
      }
   }

   @Listener
   public static class TestNonVoidReturnTypeMethodListener {
      @CacheEntryVisited
      public String callback(Event e) {
         return "Hello";
      }
   }

   @Listener
   public static class TestIncorrectMethodSignature1Listener {
      @CacheEntryVisited
      public void callback() {
      }
   }

   @Listener
   public static class TestIncorrectMethodSignature2Listener {
      @CacheEntryVisited
      public void callback(Event e, String s) {
      }
   }

   @Listener
   public static class TestIncorrectMethodSignature3Listener {
      @CacheEntryVisited
      public void callback(Event e, String... s) {
      }
   }

   @Listener
   public static class TestUnassignableMethodSignatureListener {
      @CacheEntryVisited
      public void callback(CacheEntryRemovedEvent e) {
      }
   }

   @Listener
   public static class TestPartlyUnassignableMethodSignatureListener {
      @CacheEntryVisited
      @CacheEntryRemoved
      public void callback(CacheEntryRemovedEvent e) {
      }
   }

   @Listener
   public static class TestMultipleMethodsListener {
      @CacheEntryVisited
      public void callback1(Event e) {
      }

      @CacheEntryRemoved
      public void callback2(Event e) {
      }
   }

   @Listener
   public static class TestMultipleAnnotationsOneMethodListener {
      @CacheEntryRemoved
      @CacheEntryVisited
      public void callback(Event nme) {
      }
   }

   @Listener
   public static class TestMultipleMethodsOneAnnotationListener {
      @CacheEntryVisited
      public void callback1(Event e) {
      }

      @CacheEntryVisited
      public void callback2(Event e) {
      }
   }
}
