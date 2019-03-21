package org.infinispan.notifications.impl;

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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

import javax.security.auth.Subject;
import javax.transaction.Transaction;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.ReflectionUtil;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.notifications.IncorrectListenerException;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryExpired;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.security.Security;
import org.infinispan.util.concurrent.AggregateCompletionStage;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.logging.Log;

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
      protected Class<? extends Annotation> annotation;
      protected boolean sync;
      protected ClassLoader classLoader;
      protected Subject subject;

      public Object getTarget() {
         return target;
      }

      public Method getMethod() {
         return method;
      }

      public AbstractInvocationBuilder setAnnotation(Class<? extends Annotation> annotation) {
         this.annotation = annotation;
         return this;
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

   // Processor used to handle async listener notifications.
   @Inject @ComponentName(KnownComponentNames.ASYNC_NOTIFICATION_EXECUTOR)
   protected Executor asyncProcessor;

   /**
    * Removes all listeners from the notifier
    */
   @Stop(priority = 99)
   public void stop() {
      for (List<L> list : listenersMap.values()) {
         if (list != null) list.clear();
      }
   }

   protected abstract Log getLog();

   protected abstract Map<Class<? extends Annotation>, Class<?>> getAllowedMethodAnnotations(Listener l);

   public boolean hasListener(Class<? extends Annotation> annotationClass) {
      List<L> annotations = listenersMap.get(annotationClass);
      return annotations != null && !annotations.isEmpty();
   }

   protected List<L> getListenerCollectionForAnnotation(Class<? extends Annotation> annotation) {
      List<L> list = listenersMap.get(annotation);
      if (list == null) throw new CacheException("Unknown listener annotation: " + annotation);
      return list;
   }

   public abstract CompletionStage<Void> removeListenerAsync(Object listener);

   /**
    * If the given <b>stage</b> is null or normally completed returns the provided <b>aggregateCompletionStage</b> as is.
    * Otherwise the <b>stage</b> is used as a dependant for the provided <b>aggregateCompletionStage</b> if provided or a
    * new one is created that depends upon the provided <b>stage</b>. The existing or new <b>aggregateCompletionStage</b> is then
    * returned to the caller.
    * @param aggregateCompletionStage the existing composed stage or null
    * @param stage the stage to rely upon
    * @return null or a composed stage that relies upon the provided stage
    */
   protected static AggregateCompletionStage<Void> composeStageIfNeeded(
         AggregateCompletionStage<Void> aggregateCompletionStage, CompletionStage<Void> stage) {
      if (stage != null && !CompletionStages.isCompletedSuccessfully(stage)) {
         if (aggregateCompletionStage == null) {
            aggregateCompletionStage = CompletionStages.aggregateCompletionStage();
         }
         aggregateCompletionStage.dependsOn(stage);
      }
      return aggregateCompletionStage;
   }

   protected void removeListenerFromMaps(Object listener) {
      for (Class<? extends Annotation> annotation :
            getAllowedMethodAnnotations(testListenerClassValidity(listener.getClass())).keySet())
         removeListenerInvocation(annotation, listener);
   }

   protected Set<L> removeListenerInvocation(Class<? extends Annotation> annotation, Object listener) {
      List<L> l = getListenerCollectionForAnnotation(annotation);
      Set<L> markedForRemoval = new HashSet<L>(4);
      for (L li : l) {
         if (listener.equals(li.getTarget())) markedForRemoval.add(li);
      }
      l.removeAll(markedForRemoval);
      return markedForRemoval;
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
    * @return {@code true} if annotated listener methods were found or {@code false} otherwise
    */
   protected boolean validateAndAddListenerInvocations(Object listener, AbstractInvocationBuilder builder) {
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
            for (Map.Entry<Class<? extends Annotation>, Class<?>> annotationEntry : allowedListeners.entrySet()) {
               final Class<? extends Annotation> annotationClass = annotationEntry.getKey();
               if (m.isAnnotationPresent(annotationClass)) {
                  final Class<?> eventClass = annotationEntry.getValue();
                  testListenerMethodValidity(m, eventClass, annotationClass.getName());

                  if (System.getSecurityManager() == null) {
                     m.setAccessible(true);
                  } else {
                     AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
                        m.setAccessible(true);
                        return null;
                     });
                  }

                  builder.setMethod(m);
                  builder.setAnnotation(annotationClass);
                  L invocation = builder.build();

                  getLog().tracef("Add listener invocation %s for %s", invocation, annotationClass);

                  getListenerCollectionForAnnotation(annotationClass).add(invocation);
                  foundMethods = true;
               }
            }
         }
      }

      if (!foundMethods)
         getLog().noAnnotateMethodsFoundInListener(listener.getClass());
      return foundMethods;
   }

   protected boolean validateAndAddFilterListenerInvocations(Object listener,
         AbstractInvocationBuilder builder, Set<Class<? extends Annotation>> filterAnnotations) {
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
            for (Map.Entry<Class<? extends Annotation>, Class<?>> annotationEntry : allowedListeners.entrySet()) {
               final Class<? extends Annotation> annotationClass = annotationEntry.getKey();
               if (m.isAnnotationPresent(annotationClass) && canApply(filterAnnotations, annotationClass)) {
                  final Class<?> eventClass = annotationEntry.getValue();
                  testListenerMethodValidity(m, eventClass, annotationClass.getName());

                  if (System.getSecurityManager() == null) {
                     m.setAccessible(true);
                  } else {
                     AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
                        m.setAccessible(true);
                        return null;
                     });
                  }

                  builder.setMethod(m);
                  builder.setAnnotation(annotationClass);
                  L invocation = builder.build();
                  getLog().tracef("Add listener invocation %s for %s", invocation, annotationClass);

                  getListenerCollectionForAnnotation(annotationClass).add(invocation);
                  foundMethods = true;
               }
            }
         }
      }

      if (!foundMethods)
         getLog().noAnnotateMethodsFoundInListener(listener.getClass());
      return foundMethods;
   }

   public boolean canApply(Set<Class<? extends Annotation>> filterAnnotations, Class<? extends Annotation> annotationClass) {
      // Annotations such ViewChange or TransactionCompleted should be applied regardless
      return (annotationClass != CacheEntryCreated.class
            && annotationClass != CacheEntryModified.class
            && annotationClass != CacheEntryRemoved.class
            && annotationClass != CacheEntryExpired.class)
            || (filterAnnotations.contains(annotationClass));
   }

   protected Set<Class<? extends Annotation>> findListenerCallbacks(Object listener) {
      // TODO: Partly duplicates validateAndAddListenerInvocations
      Set<Class<? extends Annotation>> listenerInterests = new HashSet<>();
      Listener l = testListenerClassValidity(listener.getClass());
      Map<Class<? extends Annotation>, Class<?>> allowedListeners = getAllowedMethodAnnotations(l);
      for (Method m : listener.getClass().getMethods()) {
         // Skip bridge methods as we don't want to count them as well.
         if (!m.isSynthetic() || !m.isBridge()) {
            // loop through all valid method annotations
            for (Map.Entry<Class<? extends Annotation>, Class<?>> annotationEntry : allowedListeners.entrySet()) {
               final Class<? extends Annotation> annotationClass = annotationEntry.getKey();
               if (m.isAnnotationPresent(annotationClass)) {
                  final Class<?> eventClass = annotationEntry.getValue();
                  testListenerMethodValidity(m, eventClass, annotationClass.getName());

                  if (System.getSecurityManager() == null) {
                     m.setAccessible(true);
                  } else {
                     AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
                        m.setAccessible(true);
                        return null;
                     });
                  }

                  listenerInterests.add(annotationClass);
               }
            }
         }
      }
      return listenerInterests;
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
         throw new IncorrectListenerException(String.format("Cache listener class %s must be annotated with org.infinispan.notifications.Listener", listenerClass.getName()));
      return l;
   }

   /**
    * Tests that a method is a valid listener method, that is that it has a single argument that is assignable to
    * <b>allowedParameter</b>. The method must also return either void or a CompletionStage, meaning the
    * method promises not block.
    * @param m method to test
    * @param allowedParameter what parameter is allowed for the method argument
    * @param annotationName name of the annotation
    * @throws IncorrectListenerException if the listener is not a valid target
    */
   protected static void testListenerMethodValidity(Method m, Class<?> allowedParameter, String annotationName) {
      if (m.getParameterTypes().length != 1 || !m.getParameterTypes()[0].isAssignableFrom(allowedParameter))
         throw new IncorrectListenerException("Methods annotated with " + annotationName + " must accept exactly one parameter, of assignable from type " + allowedParameter.getName());
      Class<?> returnType = m.getReturnType();
      if (!returnType.equals(void.class) && !CompletionStage.class.isAssignableFrom(returnType)) {
         throw new IncorrectListenerException("Methods annotated with " + annotationName + " should have a return type of void or CompletionStage.");
      }
   }

   protected abstract Transaction suspendIfNeeded();

   protected abstract void resumeIfNeeded(Transaction transaction);

   /**
    * Class that encapsulates a valid invocation for a given registered listener - containing a reference to the method
    * to be invoked as well as the target object.
    */
   protected class ListenerInvocationImpl<A> implements ListenerInvocation<A> {
      final Object target;
      final Method method;
      final boolean sync;
      final WeakReference<ClassLoader> classLoader;
      final Subject subject;

      public ListenerInvocationImpl(Object target, Method method, boolean sync, ClassLoader classLoader, Subject subject) {
         this.target = target;
         this.method = method;
         this.sync = sync;
         this.classLoader = new WeakReference<>(classLoader);
         this.subject = subject;
      }

      @Override
      public CompletionStage<Void> invoke(final A event) {
         Supplier<Object> r = () -> {
            ClassLoader contextClassLoader = null;
            Transaction transaction = suspendIfNeeded();
            if (classLoader.get() != null) {
               contextClassLoader = SecurityActions.setContextClassLoader(classLoader.get());
            }

            try {
               Object result;
               if (subject != null) {
                  try {
                     result = Security.doAs(subject, (PrivilegedExceptionAction<Object>) () -> {
                        // Don't want to print out Subject as it could have sensitive information
                        getLog().tracef("Invoking listener: %s passing event %s using subject", target, event);
                        return method.invoke(target, event);
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
                  getLog().tracef("Invoking listener: %s passing event %s", target, event);
                  result = method.invoke(target, event);
               }
               getLog().tracef("Listener %s has completed event %s", target, event);
               return result;
            } catch (InvocationTargetException exception) {
               Throwable cause = getRealException(exception);
               if (sync) {
                  throw getLog().exceptionInvokingListener(
                        cause.getClass().getName(), method, target, cause);
               } else {
                  getLog().unableToInvokeListenerMethod(method, target, cause);
               }
            } catch (IllegalAccessException exception) {
               getLog().unableToInvokeListenerMethodAndRemoveListener(method, target, exception);
               // Don't worry about return, just let it fire async
               removeListenerAsync(target);
            } finally {
               if (classLoader.get() != null) {
                  SecurityActions.setContextClassLoader(contextClassLoader);
               }
               resumeIfNeeded(transaction);
            }
            return null;
         };

         if (sync) {
            // Sync can run in a blocking (null) or non blocking (CompletionStage) fashion
            Object result = r.get();
            if (result instanceof CompletionStage) {
               return (CompletionStage<Void>) result;
            }
         } else {
            asyncProcessor.execute(r::get);
         }
         return CompletableFutures.completedNull();
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
