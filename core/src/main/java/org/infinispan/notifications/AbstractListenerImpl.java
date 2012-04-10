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
package org.infinispan.notifications;

import org.infinispan.CacheException;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.util.ReflectionUtil;
import org.infinispan.util.concurrent.WithinThreadExecutor;
import org.infinispan.util.logging.Log;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

/**
 * Functionality common to both {@link org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifierImpl} and
 * {@link org.infinispan.notifications.cachelistener.CacheNotifierImpl}
 *
 * @author Manik Surtani
 */
public abstract class AbstractListenerImpl {

   protected final Map<Class<? extends Annotation>, List<ListenerInvocation>> listenersMap = new HashMap<Class<? extends Annotation>, List<ListenerInvocation>>(16, 0.99f);


   // two separate executor services, one for sync and one for async listeners
   protected ExecutorService syncProcessor;
   protected ExecutorService asyncProcessor;


   @Inject
   void injectExecutor(@ComponentName(KnownComponentNames.ASYNC_NOTIFICATION_EXECUTOR) ExecutorService executor) {
      this.asyncProcessor = executor;
   }

   @Start (priority = 9)
   public void start() {
      syncProcessor = new WithinThreadExecutor();
   }

   /**
    * Removes all listeners from the notifier
    */
   @Stop(priority = 99)
   void stop() {
      for (List<ListenerInvocation> list : listenersMap.values()) {
         if (list != null) list.clear();
      }

      if (syncProcessor != null) syncProcessor.shutdownNow();
   }

   protected abstract Log getLog();

   protected abstract Map<Class<? extends Annotation>, Class<?>> getAllowedMethodAnnotations();

   protected List<ListenerInvocation> getListenerCollectionForAnnotation(Class<? extends Annotation> annotation) {
      List<ListenerInvocation> list = listenersMap.get(annotation);
      if (list == null) throw new CacheException("Unknown listener annotation: " + annotation);
      return list;
   }

   public void removeListener(Object listener) {
      for (Class<? extends Annotation> annotation : getAllowedMethodAnnotations().keySet())
         removeListenerInvocation(annotation, listener);
   }

   private void removeListenerInvocation(Class<? extends Annotation> annotation, Object listener) {
      if (listener == null) return;
      List<ListenerInvocation> l = getListenerCollectionForAnnotation(annotation);
      Set<Object> markedForRemoval = new HashSet<Object>(4);
      for (ListenerInvocation li : l) {
         if (listener.equals(li.target)) markedForRemoval.add(li);
      }
      l.removeAll(markedForRemoval);
   }

   public void addListener(Object listener) {
      validateAndAddListenerInvocation(listener);
   }

   public Set<Object> getListeners() {
      Set<Object> result = new HashSet<Object>(listenersMap.size());
      for (List<ListenerInvocation> list : listenersMap.values()) {
         for (ListenerInvocation li : list) result.add(li.target);
      }
      return Collections.unmodifiableSet(result);
   }

   /**
    * Loops through all valid methods on the object passed in, and caches the relevant methods as {@link
    * ListenerInvocation} for invocation by reflection.
    *
    * @param listener object to be considered as a listener.
    */
   @SuppressWarnings("unchecked")
   private void validateAndAddListenerInvocation(Object listener) {
      boolean sync = testListenerClassValidity(listener.getClass());
      boolean foundMethods = false;
      Map<Class<? extends Annotation>, Class<?>> allowedListeners = getAllowedMethodAnnotations();
      // now try all methods on the listener for anything that we like.  Note that only PUBLIC methods are scanned.
      for (Method m : listener.getClass().getMethods()) {
         // loop through all valid method annotations
         for (Map.Entry<Class<? extends Annotation>,Class<?>> annotationEntry : allowedListeners.entrySet()) {
            Class<? extends Annotation> key = annotationEntry.getKey();
            Class<?> value = annotationEntry.getValue();
            if (m.isAnnotationPresent(key)) {
               testListenerMethodValidity(m, value, key.getName());
               addListenerInvocation(key, new ListenerInvocation(listener, m, sync));
               foundMethods = true;
            }
         }
      }

      if (!foundMethods)
         getLog().noAnnotateMethodsFoundInListener(listener.getClass());
   }

   private void addListenerInvocation(Class<? extends Annotation> annotation, ListenerInvocation li) {
      List<ListenerInvocation> result = getListenerCollectionForAnnotation(annotation);
      result.add(li);
   }

   /**
    * Tests if a class is properly annotated as a CacheListener and returns whether callbacks on this class should be
    * invoked synchronously or asynchronously.
    *
    * @param listenerClass class to inspect
    * @return true if callbacks on this class should use the syncProcessor; false if it should use the asyncProcessor.
    */
   protected static boolean testListenerClassValidity(Class<?> listenerClass) {
      Listener l = ReflectionUtil.getAnnotation(listenerClass, Listener.class);
      if (l == null)
         throw new IncorrectListenerException(String.format("Cache listener class %s must be annotated with org.infinispan.notifications.annotation.Listener", listenerClass.getName()));
      if (!Modifier.isPublic(listenerClass.getModifiers()))
         throw new IncorrectListenerException(String.format("Cache listener class %s must be public!", listenerClass.getName()));
      return l.sync();
   }

   protected static void testListenerMethodValidity(Method m, Class<?> allowedParameter, String annotationName) {
      if (m.getParameterTypes().length != 1 || !m.getParameterTypes()[0].isAssignableFrom(allowedParameter))
         throw new IncorrectListenerException("Methods annotated with " + annotationName + " must accept exactly one parameter, of assignable from type " + allowedParameter.getName());
      if (!m.getReturnType().equals(void.class))
         throw new IncorrectListenerException("Methods annotated with " + annotationName + " should have a return type of void.");
   }

   /**
    * Class that encapsulates a valid invocation for a given registered listener - containing a reference to the method
    * to be invoked as well as the target object.
    */
   protected class ListenerInvocation {
      public final Object target;
      public final Method method;
      public final boolean sync;

      public ListenerInvocation(Object target, Method method, boolean sync) {
         this.target = target;
         this.method = method;
         this.sync = sync;
      }

      public void invoke(final Object event) {
         Runnable r = new Runnable() {

            public void run() {
               try {
                  method.invoke(target, event);
               }
               catch (InvocationTargetException exception) {
                  Throwable cause = getRealException(exception);
                  if (sync) {
                     throw new CacheException(String.format(
                        "Caught exception [%s] while invoking method [%s] on listener instance: %s"
                        , cause.getClass().getName(), method, target
                     ), cause);
                  } else {
                     getLog().unableToInvokeListenerMethod(method, target, cause);
                  }
               }
               catch (IllegalAccessException exception) {
                  getLog().unableToInvokeListenerMethod(method, target, exception);
                  removeListener(target);
               }
            }
         };

         if (sync)
            syncProcessor.execute(r);
         else
            asyncProcessor.execute(r);
      }
   }

   private Throwable getRealException(Throwable re) {
      if (re.getCause() == null) return re;
      Throwable cause = re.getCause();
      if (cause instanceof CacheException || cause instanceof RuntimeException)
         return getRealException(cause);
      else
         return re;
   }

}
