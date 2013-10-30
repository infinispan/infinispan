package org.infinispan.notifications;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.ReflectionUtil;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.notifications.cachelistener.event.EventImpl;
import org.infinispan.util.concurrent.WithinThreadExecutor;
import org.infinispan.util.logging.Log;

import javax.security.auth.Subject;
import javax.transaction.Transaction;

import java.lang.annotation.Annotation;
import java.lang.ref.WeakReference;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
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
      addListener(listener, null);
   }

   public void addListener(Object listener, ClassLoader classLoader) {
      validateAndAddListenerInvocation(listener, null, null, classLoader);
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
   protected void validateAndAddListenerInvocation(Object listener, KeyValueFilter filter, Converter converter,
                                                   ClassLoader classLoader) {
      Listener l = testListenerClassValidity(listener.getClass());
      UUID generatedId = UUID.randomUUID();
      boolean foundMethods = false;
      boolean foundClusterMethods = false;
      Map<Class<? extends Annotation>, Class<?>> allowedListeners = getAllowedMethodAnnotations();
      // now try all methods on the listener for anything that we like.  Note that only PUBLIC methods are scanned.
      for (Method m : listener.getClass().getMethods()) {
         // loop through all valid method annotations
         for (Map.Entry<Class<? extends Annotation>,Class<?>> annotationEntry : allowedListeners.entrySet()) {
            Class<? extends Annotation> key = annotationEntry.getKey();
            Class<?> value = annotationEntry.getValue();
            if (m.isAnnotationPresent(key)) {
               testListenerMethodValidity(m, value, key.getName());
               m.setAccessible(true);
               addListenerInvocation(key, createListenerInvocation(listener, m, l, filter, converter, classLoader, generatedId, Subject.getSubject(AccessController.getContext())));
               foundMethods = true;
               // If the annotation is also a cluster listener available event we need to replicate listener
               if (l.clustered()) {
                  foundClusterMethods = true;
               }
            }
         }
      }

      if (!foundMethods)
         getLog().noAnnotateMethodsFoundInListener(listener.getClass());
      else {
         addedListener(listener, generatedId, foundClusterMethods, filter, converter);
      }
   }

   protected ListenerInvocation createListenerInvocation(Object listener, Method m, Listener l, KeyValueFilter filter,
                                                         Converter converter, ClassLoader classLoader, UUID generatedId, Subject subject) {
      return new ListenerInvocation(listener, m, l.sync(), l.primaryOnly(), l.clustered(), filter, converter,
                                    classLoader, generatedId, subject);
   }

   protected void addedListener(Object listener, UUID generatedId, boolean hasClusteredMethods, KeyValueFilter filter,
                                Converter converter) {
      // Default is noop
   }

   private void addListenerInvocation(Class<? extends Annotation> annotation, ListenerInvocation li) {
      List<ListenerInvocation> result = getListenerCollectionForAnnotation(annotation);
      result.add(li);
   }

   /**
    * Tests if a class is properly annotated as a CacheListener and returns the Listener annotation.
    *
    * @param listenerClass class to inspect
    * @return the Listener annotation
    */
   protected static Listener testListenerClassValidity(Class<?> listenerClass) {
      Listener l = ReflectionUtil.getAnnotation(listenerClass, Listener.class);
      if (l == null)
         throw new IncorrectListenerException(String.format("Cache listener class %s must be annotated with org.infinispan.notifications.annotation.Listener", listenerClass.getName()));
      return l;
   }

   protected static void testListenerMethodValidity(Method m, Class<?> allowedParameter, String annotationName) {
      if (m.getParameterTypes().length != 1 || !m.getParameterTypes()[0].isAssignableFrom(allowedParameter))
         throw new IncorrectListenerException("Methods annotated with " + annotationName + " must accept exactly one parameter, of assignable from type " + allowedParameter.getName());
      if (!m.getReturnType().equals(void.class))
         throw new IncorrectListenerException("Methods annotated with " + annotationName + " should have a return type of void.");
   }

   protected abstract Transaction suspendIfNeeded();

   protected abstract void resumeIfNeeded(Transaction transaction);

   /**
    * Class that encapsulates a valid invocation for a given registered listener - containing a reference to the method
    * to be invoked as well as the target object.
    */
   protected class ListenerInvocation {
      public final Object target;
      public final Method method;
      public final boolean sync;
      public final boolean onlyPrimary;
      public final boolean clustered;
      public final WeakReference<ClassLoader> classLoader;
      public final KeyValueFilter filter;
      public final Converter converter;
      public final UUID generatedId;
      public final Subject subject;

      public ListenerInvocation(Object target, Method method, boolean sync, boolean onlyPrimary, boolean clustered,
                                KeyValueFilter filter, Converter converter, ClassLoader classLoader, UUID generatedId, Subject subject) {
         this.target = target;
         this.method = method;
         this.sync = sync;
         this.onlyPrimary = onlyPrimary;
         this.clustered = clustered;
         this.filter = filter;
         this.converter = converter;
         this.classLoader = new WeakReference<ClassLoader>(classLoader);
         this.generatedId = generatedId;
         this.subject = subject;
      }

      public void invoke(final Object event) {
         invoke(event, false, true);
      }

      public void invoke(final Object event, boolean isLocalNodePrimaryOwner) {
         invoke(event, isLocalNodePrimaryOwner, false);
      }

      public void invoke(final Object event, boolean isLocalNodePrimaryOwner, final boolean unKeyed) {
         if (unKeyed || shouldInvoke(event, isLocalNodePrimaryOwner)) {
            Runnable r = new Runnable() {

               @Override
               public void run() {
                  ClassLoader contextClassLoader = null;
                  Transaction transaction = suspendIfNeeded();
                  if (classLoader != null && classLoader.get() != null) {
                     contextClassLoader = setContextClassLoader(classLoader.get());
                  }
                  try {
                     // Only run converter on events we can change and if it was keyed
                     if (!unKeyed && converter != null) {
                        if (event instanceof EventImpl) {
                           EventImpl eventImpl = (EventImpl)event;
                           eventImpl.setValue(converter.convert(eventImpl.getKey(), eventImpl.getValue(),
                                                                eventImpl.getMetadata()));
                        } else {
                           throw new IllegalArgumentException("Provided event should be EventImpl when a converter is" +
                                                                    "being used!");
                        }
                     }
                     if (subject != null) {
                        try {
                           Subject.doAs(subject, new PrivilegedExceptionAction<Void>() {
                              @Override
                              public Void run() throws Exception {
                                 method.invoke(target, event);
                                 return null;
                              }
                           });
                        } catch (PrivilegedActionException e) {
                           Throwable cause = e.getCause();
                           if (cause instanceof InvocationTargetException) {
                              throw (InvocationTargetException)cause;
                           } else if (cause instanceof IllegalAccessException) {
                              throw (IllegalAccessException)cause;
                           } else {
                              throw new InvocationTargetException(cause);
                           }
                        }
                     } else {
                        method.invoke(target, event);
                     }
                  } catch (InvocationTargetException exception) {
                     Throwable cause = getRealException(exception);
                     if (sync) {
                        throw getLog().exceptionInvokingListener(
                              cause.getClass().getName(), method, target, cause);
                     } else {
                        getLog().unableToInvokeListenerMethod(method, target, cause);
                     }
                  } catch (IllegalAccessException exception) {
                     getLog().unableToInvokeListenerMethod(method, target, exception);
                     removeListener(target);
                  } finally {
                     if (classLoader != null && classLoader.get() != null) {
                        setContextClassLoader(contextClassLoader);
                     }
                     resumeIfNeeded(transaction);
                  }
               }
            };

            if (sync)
               syncProcessor.execute(r);
            else
               asyncProcessor.execute(r);
         }
      }

      private boolean shouldInvoke(Object event, boolean isLocalNodePrimaryOwner) {
         if (onlyPrimary && !isLocalNodePrimaryOwner) return false;
         if (event instanceof  EventImpl) {
            EventImpl eventImpl = (EventImpl)event;
            // Cluster listeners only get post events
            if (eventImpl.isPre() && clustered) return false;
            if (filter != null && !filter.accept(eventImpl.getKey(), eventImpl.getValue(), eventImpl.getMetadata())) return false;
         }
         return true;
      }
   }

   private Throwable getRealException(Throwable re) {
      if (re.getCause() == null) return re;
      Throwable cause = re.getCause();
      if (cause instanceof RuntimeException || cause instanceof Error)
         return getRealException(cause);
      else
         return re;
   }

   static ClassLoader setContextClassLoader(final ClassLoader loader) {
      PrivilegedAction<ClassLoader> action = new PrivilegedAction<ClassLoader>() {
         @Override
         public ClassLoader run() {
            ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
            Thread.currentThread().setContextClassLoader(loader);
            return contextClassLoader;
         }
      };
      return AccessController.doPrivileged(action);
   }
}
