package org.infinispan.notifications.impl;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.ReflectionUtil;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.notifications.IncorrectListenerException;
import org.infinispan.notifications.Listener;
import org.infinispan.security.Security;
import org.infinispan.util.concurrent.WithinThreadExecutor;
import org.infinispan.util.logging.Log;

import javax.security.auth.Subject;
import javax.transaction.Transaction;

import java.lang.annotation.Annotation;
import java.lang.ref.WeakReference;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
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
 * @param <T> Defines the type of event that will be used by the subclass
 * @param <L> Defines the type of ListenerInvocations that the subclasses use
 * @author Manik Surtani
 * @author William Burns
 */
public abstract class AbstractListenerImpl<T, L extends ListenerInvocation<T>> {

   protected final Map<Class<? extends Annotation>, List<L>> listenersMap = new HashMap<>(16, 0.99f);

   protected abstract class AbstractInvocationBuilder {
      protected Object target;
      protected Method method;
      protected boolean sync;
      protected ClassLoader classLoader;
      protected Subject subject;

      public Object getTarget() {
         return target;
      }

      public Method getMethod() {
         return method;
      }

      public boolean isSync() {
         return sync;
      }

      public ClassLoader getClassLoader() {
         return classLoader;
      }

      public Subject getSubject() {
         return subject;
      }

      public AbstractInvocationBuilder setTarget(Object target) {
         this.target = target;
         return this;
      }

      public AbstractInvocationBuilder setMethod(Method method) {
         this.method = method;
         return this;
      }

      public AbstractInvocationBuilder setSync(boolean sync) {
         this.sync = sync;
         return this;
      }

      public AbstractInvocationBuilder setClassLoader(ClassLoader classLoader) {
         this.classLoader = classLoader;
         return this;
      }

      public AbstractInvocationBuilder setSubject(Subject subject) {
         this.subject = subject;
         return this;
      }

      public abstract L build();

   }

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
      for (List<L> list : listenersMap.values()) {
         if (list != null) list.clear();
      }

      if (syncProcessor != null) syncProcessor.shutdownNow();
   }

   protected abstract Log getLog();

   protected abstract Map<Class<? extends Annotation>, Class<?>> getAllowedMethodAnnotations(Listener l);

   protected List<L> getListenerCollectionForAnnotation(Class<? extends Annotation> annotation) {
      List<L> list = listenersMap.get(annotation);
      if (list == null) throw new CacheException("Unknown listener annotation: " + annotation);
      return list;
   }

   public void removeListener(Object listener) {
      for (Class<? extends Annotation> annotation :
            getAllowedMethodAnnotations(testListenerClassValidity(listener.getClass())).keySet())
         removeListenerInvocation(annotation, listener);
   }

   private void removeListenerInvocation(Class<? extends Annotation> annotation, Object listener) {
      if (listener == null) return;
      List<L> l = getListenerCollectionForAnnotation(annotation);
      Set<ListenerInvocation> markedForRemoval = new HashSet<ListenerInvocation>(4);
      for (ListenerInvocation li : l) {
         if (listener.equals(li.getTarget())) markedForRemoval.add(li);
      }
      l.removeAll(markedForRemoval);
   }

   public Set<Object> getListeners() {
      Set<Object> result = new HashSet<Object>(listenersMap.size());
      for (List<L> list : listenersMap.values()) {
         for (ListenerInvocation li : list) result.add(li.getTarget());
      }
      return Collections.unmodifiableSet(result);
   }

   /**
    * Loops through all valid methods on the object passed in, and caches the relevant methods as {@link
    * ListenerInvocation} for invocation by reflection.
    * The builder provided will be used to create the listener invocations.  This method will set the target, subject
    * sync, and methods as needed.  If other values are needed to be set they should be invoked before passing to this method.
    *
    * @param listener object to be considered as a listener.
    * @param builder The builder to use to build the invocation
    */
   protected boolean validateAndAddListenerInvocation(Object listener, AbstractInvocationBuilder builder) {
      Listener l = testListenerClassValidity(listener.getClass());
      boolean foundMethods = false;
      builder.setTarget(listener);
      builder.setSubject(Security.getSubject());
      builder.setSync(l.sync());
      Map<Class<? extends Annotation>, Class<?>> allowedListeners = getAllowedMethodAnnotations(l);
      // now try all methods on the listener for anything that we like.  Note that only PUBLIC methods are scanned.
      for (Method m : listener.getClass().getMethods()) {
         // Skip bridge methods as we don't want to count them as well.
         if (!m.isSynthetic() || !m.isBridge()) {
            // loop through all valid method annotations
            for (Map.Entry<Class<? extends Annotation>,Class<?>> annotationEntry : allowedListeners.entrySet()) {
               Class<? extends Annotation> key = annotationEntry.getKey();
               Class<?> value = annotationEntry.getValue();
               if (m.isAnnotationPresent(key)) {
                  testListenerMethodValidity(m, value, key.getName());
                  m.setAccessible(true);
                  builder.setMethod(m);
                  addListenerInvocation(key, builder.build());
                  foundMethods = true;
               }
            }
         }
      }

      if (!foundMethods)
         getLog().noAnnotateMethodsFoundInListener(listener.getClass());
      return foundMethods;
   }

   private void addListenerInvocation(Class<? extends Annotation> annotation, L li) {
      List<L> result = getListenerCollectionForAnnotation(annotation);
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
   protected class ListenerInvocationImpl<A> implements ListenerInvocation<A> {
      public final Object target;
      public final Method method;
      public final boolean sync;
      public final WeakReference<ClassLoader> classLoader;
      public final Subject subject;

      public ListenerInvocationImpl(Object target, Method method, boolean sync, ClassLoader classLoader, Subject subject) {
         this.target = target;
         this.method = method;
         this.sync = sync;
         this.classLoader = new WeakReference<ClassLoader>(classLoader);
         this.subject = subject;
      }

      @Override
      public void invoke(final A event) {
         Runnable r = new Runnable() {

            @Override
            public void run() {
               ClassLoader contextClassLoader = null;
               Transaction transaction = suspendIfNeeded();
               if (classLoader != null && classLoader.get() != null) {
                  contextClassLoader = SecurityActions.setContextClassLoader(classLoader.get());
               }
               try {
                  if (subject != null) {
                     try {
                        Security.doAs(subject, new PrivilegedExceptionAction<Void>() {
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
                     SecurityActions.setContextClassLoader(contextClassLoader);
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

      @Override
      public Object getTarget() {
         return target;
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
}
