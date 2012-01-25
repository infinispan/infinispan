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
package org.infinispan.notifications.cachelistener;

import org.infinispan.notifications.IncorrectListenerException;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryVisited;
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.test.AbstractInfinispanTest;

import static org.testng.AssertJUnit.*;
import org.testng.annotations.Test;

import java.util.List;

@Test(groups = "unit", testName = "notifications.cachelistener.ListenerRegistrationTest")
public class ListenerRegistrationTest extends AbstractInfinispanTest {
   public void testControl() {
      Object l = new TestControlListener();
      CacheNotifierImpl n = new CacheNotifierImpl();
      n.addListener(l);
      assertEquals(1, n.getListeners().size());
   }

   public void testCacheListenerNoMethods() {
      Object l = new TestCacheListenerNoMethodsListener();
      CacheNotifierImpl n = new CacheNotifierImpl();
      n.addListener(l);
      assertEquals("Hello", l.toString());
      assertTrue("No listeners should be registered.", n.getListeners().isEmpty()); // since the valid listener has no methods to listen
   }

   public void testNonAnnotatedListener() {
      Object l = new TestNonAnnotatedListener();
      CacheNotifierImpl n = new CacheNotifierImpl();
      try {
         n.addListener(l);
         fail("Should not accept an un-annotated cache listener");
      }
      catch (IncorrectListenerException icle) {
         // expected
      }
      assertTrue("No listeners should be registered.", n.getListeners().isEmpty());
   }

   public void testNonPublicListener() {
      Object l = new TestNonPublicListener();
      CacheNotifierImpl n = new CacheNotifierImpl();
      try {
         n.addListener(l);
         fail("Should not accept a private callback class");
      }
      catch (IncorrectListenerException icle) {
         // expected
      }
      assertTrue("No listeners should be registered.", n.getListeners().isEmpty());
   }

   public void testNonPublicListenerMethod() {
      Object l = new TestNonPublicListenerMethodListener();
      CacheNotifierImpl n = new CacheNotifierImpl();
      n.addListener(l);

      // should not fail, should just not register anything

      assertTrue("No listeners should be registered.", n.getListeners().isEmpty());
   }

   public void testNonVoidReturnTypeMethod() {
      Object l = new TestNonVoidReturnTypeMethodListener();
      CacheNotifierImpl n = new CacheNotifierImpl();
      try {
         n.addListener(l);
         fail("Should not accept a listener method with a return type");
      }
      catch (IncorrectListenerException icle) {
         // expected
      }
      assertTrue("No listeners should be registered.", n.getListeners().isEmpty());
   }

   public void testIncorrectMethodSignature1() {
      Object l = new TestIncorrectMethodSignature1Listener();
      CacheNotifierImpl n = new CacheNotifierImpl();
      try {
         n.addListener(l);
         fail("Should not accept a cache listener with a bad method signature");
      }
      catch (IncorrectListenerException icle) {
         // expected
      }
      assertTrue("No listeners should be registered.", n.getListeners().isEmpty());
   }

   public void testIncorrectMethodSignature2() {
      Object l = new TestIncorrectMethodSignature2Listener();
      CacheNotifierImpl n = new CacheNotifierImpl();
      try {
         n.addListener(l);
         fail("Should not accept a cache listener with a bad method signature");
      }
      catch (IncorrectListenerException icle) {
         // expected
      }
      assertTrue("No listeners should be registered.", n.getListeners().isEmpty());
   }

   public void testIncorrectMethodSignature3() {
      Object l = new TestIncorrectMethodSignature3Listener();
      CacheNotifierImpl n = new CacheNotifierImpl();
      try {
         n.addListener(l);
         fail("Should not accept a cache listener with a bad method signature");
      }
      catch (IncorrectListenerException icle) {
         // expected
      }
      assertTrue("No listeners should be registered.", n.getListeners().isEmpty());
   }

   public void testUnassignableMethodSignature() {
      Object l = new TestUnassignableMethodSignatureListener();
      CacheNotifierImpl n = new CacheNotifierImpl();
      try {
         n.addListener(l);
         fail("Should not accept a cache listener with a bad method signature");
      }
      catch (IncorrectListenerException icle) {
         // expected
      }
      assertTrue("No listeners should be registered.", n.getListeners().isEmpty());
   }

   public void testPartlyUnassignableMethodSignature() {
      Object l = new TestPartlyUnassignableMethodSignatureListener();
      CacheNotifierImpl n = new CacheNotifierImpl();
      try {
         n.addListener(l);
         fail("Should not accept a cache listener with a bad method signature");
      }
      catch (IncorrectListenerException icle) {
         // expected
      }
   }

   public void testMultipleMethods() {
      Object l = new TestMultipleMethodsListener();
      CacheNotifierImpl n = new CacheNotifierImpl();
      n.addListener(l);
      List invocations = n.cacheEntryVisitedListeners;
      assertEquals(1, invocations.size());
      invocations = n.cacheEntryRemovedListeners;
      assertEquals(1, invocations.size());
      assertEquals(1, n.getListeners().size());
   }

   public void testMultipleAnnotationsOneMethod() {
      Object l = new TestMultipleAnnotationsOneMethodListener();
      CacheNotifierImpl n = new CacheNotifierImpl();
      n.addListener(l);
      List invocations = n.cacheEntryVisitedListeners;
      assertEquals(1, invocations.size());
      invocations = n.cacheEntryRemovedListeners;
      assertEquals(1, invocations.size());
      assertEquals(1, n.getListeners().size());
   }

   public void testMultipleMethodsOneAnnotation() {
      Object l = new TestMultipleMethodsOneAnnotationListener();
      CacheNotifierImpl n = new CacheNotifierImpl();
      n.addListener(l);
      List invocations = n.cacheEntryVisitedListeners;
      assertEquals(2, invocations.size());
      assertEquals(1, n.getListeners().size());
   }

   @Listener
   static public class TestControlListener {
      @CacheEntryVisited
      @CacheEntryRemoved
      public void callback(Event e) {
      }
   }

   @Listener
   static public class TestCacheListenerNoMethodsListener {
      public String toString() {
         return "Hello";
      }
   }

   static public class TestNonAnnotatedListener {
      public String toString() {
         return "Hello";
      }
   }

   @Listener
   static protected class TestNonPublicListener {
      @CacheEntryVisited
      public void callback() {
      }
   }

   @Listener
   static public class TestNonPublicListenerMethodListener {
      @CacheEntryVisited
      protected void callback(Event e) {
      }
   }

   @Listener
   static public class TestNonVoidReturnTypeMethodListener {
      @CacheEntryVisited
      public String callback(Event e) {
         return "Hello";
      }
   }

   @Listener
   static public class TestIncorrectMethodSignature1Listener {
      @CacheEntryVisited
      public void callback() {
      }
   }

   @Listener
   static public class TestIncorrectMethodSignature2Listener {
      @CacheEntryVisited
      public void callback(Event e, String s) {
      }
   }

   @Listener
   static public class TestIncorrectMethodSignature3Listener {
      @CacheEntryVisited
      public void callback(Event e, String... s) {
      }
   }

   @Listener
   static public class TestUnassignableMethodSignatureListener {
      @CacheEntryVisited
      public void callback(CacheEntryRemovedEvent e) {
      }
   }

   @Listener
   static public class TestPartlyUnassignableMethodSignatureListener {
      @CacheEntryVisited
      @CacheEntryRemoved
      public void callback(CacheEntryRemovedEvent e) {
      }
   }

   @Listener
   static public class TestMultipleMethodsListener {
      @CacheEntryVisited
      public void callback1(Event e) {
      }

      @CacheEntryRemoved
      public void callback2(Event e) {
      }
   }

   @Listener
   static public class TestMultipleAnnotationsOneMethodListener {
      @CacheEntryRemoved
      @CacheEntryVisited
      public void callback(Event nme) {
      }
   }

   @Listener
   static public class TestMultipleMethodsOneAnnotationListener {
      @CacheEntryVisited
      public void callback1(Event e) {
      }

      @CacheEntryVisited
      public void callback2(Event e) {
      }
   }
}
