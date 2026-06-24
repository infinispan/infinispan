package org.infinispan.interceptors.impl;

import static org.infinispan.commons.util.Immutables.immutableListAdd;
import static org.infinispan.commons.util.Immutables.immutableListRemove;
import static org.infinispan.commons.util.Immutables.immutableListReplace;
import static org.infinispan.util.logging.Log.CONTAINER;

import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.TimeoutException;
import org.infinispan.commons.util.ImmutableListCopy;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.interceptors.AsyncInterceptor;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.interceptors.BaseAsyncInterceptor;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.interceptors.ExceptionSyncInvocationStage;
import org.infinispan.interceptors.InvocationStage;
import org.infinispan.interceptors.Skip;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Knows how to build and manage a chain of interceptors. Also in charge with invoking methods on the chain.
 *
 * @author Dan Berindei
 * @since 9.0
 */
@Scope(Scopes.NAMED_CACHE)
@SuppressWarnings("deprecation")
public class AsyncInterceptorChainImpl implements AsyncInterceptorChain {
   // Using the same list type everywhere may help with the optimization of the invocation context methods
   private static final ImmutableListCopy<AsyncInterceptor> EMPTY_INTERCEPTORS_LIST =
         new ImmutableListCopy<>();
   private static final Log log = LogFactory.getLog(AsyncInterceptorChainImpl.class);

   private final ReentrantLock lock = new ReentrantLock();

   @Inject ComponentRegistry componentRegistry;

   // Modifications are guarded with "lock", but reads do not need synchronization
   private volatile List<AsyncInterceptor> interceptors = EMPTY_INTERCEPTORS_LIST;
   private volatile AsyncInterceptor firstInterceptor = null;
   // Holds the first interceptor that actually does something for visitGetKeyValueCommand
   private volatile DDAsyncInterceptor firstGetKeyValueInterceptor = null;

   @Start
   void printChainInfo() {
      if (log.isDebugEnabled()) {
         log.debugf("Interceptor chain size: %d", size());
         log.debugf("Interceptor chain is: %s", toString());
      }
   }

   private void validateCustomInterceptor(Class<? extends AsyncInterceptor> i) {
      // Do nothing, custom interceptors extending internal interceptors no longer "inherit" the annotations
   }

   /**
    * Ensures that the interceptor of type passed in isn't already added
    *
    * @param clazz type of interceptor to check for
    */
   private void checkInterceptor(Class<? extends AsyncInterceptor> clazz) {
      if (containsInterceptorType(clazz, false))
         throw new CacheConfigurationException("Detected interceptor of type [" + clazz.getName() +
                                                     "] being added to the interceptor chain " +
                                                     System.identityHashCode(this) + " more than once!");
   }

   @Override
   public void addInterceptor(AsyncInterceptor interceptor, int position) {
      final ReentrantLock lock = this.lock;
      lock.lock();
      try {
         Class<? extends AsyncInterceptor> interceptorClass = interceptor.getClass();
         checkInterceptor(interceptorClass);
         validateCustomInterceptor(interceptorClass);
         interceptors = immutableListAdd(interceptors, position, interceptor);
         rebuildInterceptors();
      } finally {
         lock.unlock();
      }
   }

   @Override
   public void removeInterceptor(int position) {
      final ReentrantLock lock = this.lock;
      lock.lock();
      try {
         interceptors = immutableListRemove(interceptors, position);
         rebuildInterceptors();
      } finally {
         lock.unlock();
      }
   }

   @Override
   public int size() {
      return interceptors.size();
   }

   @Override
   public void removeInterceptor(Class<? extends AsyncInterceptor> clazz) {
      final ReentrantLock lock = this.lock;
      lock.lock();
      try {
         for (int i = 0; i < interceptors.size(); i++) {
            if (interceptorMatches(interceptors.get(i), clazz)) {
               removeInterceptor(i);
               break;
            }
         }
      } finally {
         lock.unlock();
      }
   }

   private boolean interceptorMatches(AsyncInterceptor interceptor,
                                        Class<? extends AsyncInterceptor> clazz) {
      Class<? extends AsyncInterceptor> interceptorType = interceptor.getClass();
      return clazz == interceptorType;
   }

   @Override
   public boolean addInterceptorAfter(AsyncInterceptor toAdd,
                                      Class<? extends AsyncInterceptor> afterInterceptor) {
      lock.lock();
      try {
         Class<? extends AsyncInterceptor> interceptorClass = toAdd.getClass();
         checkInterceptor(interceptorClass);
         validateCustomInterceptor(interceptorClass);
         for (int i = 0; i < interceptors.size(); i++) {
            if (interceptorMatches(interceptors.get(i), afterInterceptor)) {
               interceptors = immutableListAdd(interceptors, i + 1, toAdd);
               rebuildInterceptors();
               return true;
            }
         }
         return false;
      } finally {
         lock.unlock();
      }
   }

   @Override
   public boolean addInterceptorBefore(AsyncInterceptor toAdd,
                                       Class<? extends AsyncInterceptor> beforeInterceptor) {
      lock.lock();
      try {
         Class<? extends AsyncInterceptor> interceptorClass = toAdd.getClass();
         checkInterceptor(interceptorClass);
         validateCustomInterceptor(interceptorClass);
         for (int i = 0; i < interceptors.size(); i++) {
            if (interceptorMatches(interceptors.get(i), beforeInterceptor)) {
               interceptors = immutableListAdd(interceptors, i, toAdd);
               rebuildInterceptors();
               return true;
            }
         }
         return false;
      } finally {
         lock.unlock();
      }
   }

   @Override
   public boolean replaceInterceptor(AsyncInterceptor replacingInterceptor,
                                     Class<? extends AsyncInterceptor> existingInterceptorType) {
      final ReentrantLock lock = this.lock;
      lock.lock();
      try {
         Class<? extends AsyncInterceptor> interceptorClass = replacingInterceptor.getClass();
         checkInterceptor(interceptorClass);
         validateCustomInterceptor(interceptorClass);

         for (int i = 0; i < interceptors.size(); i++) {
            if (interceptorMatches(interceptors.get(i), existingInterceptorType)) {
               interceptors = immutableListReplace(interceptors, i, replacingInterceptor);
               rebuildInterceptors();
               return true;
            }
         }
         return false;
      } finally {
         lock.unlock();
      }
   }

   @Override
   public void appendInterceptor(AsyncInterceptor ci, boolean isCustom) {
      lock.lock();
      try {
         Class<? extends AsyncInterceptor> interceptorClass = ci.getClass();
         if (isCustom)
            validateCustomInterceptor(interceptorClass);
         checkInterceptor(interceptorClass);
         // Called when building interceptor chain and so concurrent start calls are protected already
         interceptors = immutableListAdd(interceptors, interceptors.size(), ci);
         rebuildInterceptors();
      } finally {
         lock.unlock();
      }
   }

   @Override
   public CompletableFuture<Object> invokeAsync(InvocationContext ctx, VisitableCommand command) {
      try {
         Object result = firstInterceptor.visitCommand(ctx, command);
         if (result instanceof InvocationStage) {
            return ((InvocationStage) result).toCompletableFuture();
         } else {
            // Don't allocate future if result was already null
            if (result == null) {
               return CompletableFutures.completedNull();
            }
            return CompletableFuture.completedFuture(result);
         }
      } catch (Throwable t) {
         return CompletableFuture.failedFuture(t);
      }
   }

   @Override
   public Object invokeGet(InvocationContext ctx, GetKeyValueCommand command) {
      try {
         checkComponentStatus();
         Object result = firstGetKeyValueInterceptor.visitGetKeyValueCommand(ctx, command);
         if (result instanceof InvocationStage) {
            return ((InvocationStage) result).get();
         } else {
            return result;
         }
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new CacheException(e);
      } catch (TimeoutException e) {
         throw new TimeoutException(e.getMessage(), e);
      } catch (RuntimeException e) {
         if (ctx.isOriginLocal() && !(e instanceof CacheException)) {
            throw new CacheException(e);
         }
         throw e;
      } catch (Throwable throwable) {
         throw new CacheException(throwable);
      }
   }

   @Override
   public CompletableFuture<Object> invokeGetAsync(InvocationContext ctx, GetKeyValueCommand command) {
      try {
         checkComponentStatus();
         Object result = firstGetKeyValueInterceptor.visitGetKeyValueCommand(ctx, command);
         if (result instanceof InvocationStage) {
            return ((InvocationStage) result).toCompletableFuture();
         } else {
            if (result == null) {
               return CompletableFutures.completedNull();
            }
            return CompletableFuture.completedFuture(result);
         }
      } catch (Throwable t) {
         return CompletableFuture.failedFuture(t);
      }
   }

   @Override
   public Object invokeGetCacheEntry(InvocationContext ctx, GetCacheEntryCommand command) {
      try {
         checkComponentStatus();
         Object result = firstGetKeyValueInterceptor.visitGetCacheEntryCommand(ctx, command);
         if (result instanceof InvocationStage) {
            return ((InvocationStage) result).get();
         } else {
            return result;
         }
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new CacheException(e);
      } catch (TimeoutException e) {
         throw new TimeoutException(e.getMessage(), e);
      } catch (RuntimeException e) {
         if (ctx.isOriginLocal() && !(e instanceof CacheException)) {
            throw new CacheException(e);
         }
         throw e;
      } catch (Throwable throwable) {
         throw new CacheException(throwable);
      }
   }

   @Override
   public CompletableFuture<Object> invokeGetCacheEntryAsync(InvocationContext ctx, GetCacheEntryCommand command) {
      try {
         checkComponentStatus();
         Object result = firstGetKeyValueInterceptor.visitGetCacheEntryCommand(ctx, command);
         if (result instanceof InvocationStage) {
            return ((InvocationStage) result).toCompletableFuture();
         } else {
            if (result == null) {
               return CompletableFutures.completedNull();
            }
            return CompletableFuture.completedFuture(result);
         }
      } catch (Throwable t) {
         return CompletableFuture.failedFuture(t);
      }
   }

   private void checkComponentStatus() {
      ComponentStatus status = componentRegistry.getStatus();
      if (!status.allowInvocations()) {
         String prefix = "Cache '" + componentRegistry.getCacheName() + "'";
         switch (status) {
            case FAILED:
            case TERMINATED:
               throw CONTAINER.cacheIsTerminated(prefix, status.toString());
            case STOPPING:
               throw CONTAINER.cacheIsStopping(prefix);
            case INITIALIZING:
               LocalTopologyManager ltm = componentRegistry.getComponent(LocalTopologyManager.class);
               if (ltm != null) ltm.assertTopologyStable(componentRegistry.getCacheName());
               break;
            default:
               break;
         }
      }
   }

   @Override
   public InvocationStage invokeStage(InvocationContext ctx, VisitableCommand command) {
      try {
         return InvocationStage.makeStage(firstInterceptor.visitCommand(ctx, command));
      } catch (Throwable t) {
         return new ExceptionSyncInvocationStage(t);
      }
   }

   @Override
   public Object invoke(InvocationContext ctx, VisitableCommand command) {
      try {
         Object result = firstInterceptor.visitCommand(ctx, command);
         if (result instanceof InvocationStage) {
            return ((InvocationStage) result).get();
         } else {
            return result;
         }
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new CacheException(e);
      } catch (TimeoutException e) {
         // Create a new exception here for easier debugging
         throw new TimeoutException(e.getMessage(), e);
      } catch (RuntimeException e) {
         throw e;
      } catch (Throwable throwable) {
         throw new CacheException(throwable);
      }
   }

   @Override
   public <T extends AsyncInterceptor> T findInterceptorExtending(Class<T> interceptorClass) {
      List<AsyncInterceptor> localInterceptors = this.interceptors;
      for (AsyncInterceptor interceptor : localInterceptors) {
         boolean isSubclass = interceptorClass.isInstance(interceptor);
         if (isSubclass) {
            return interceptorClass.cast(interceptor);
         }
      }
      return null;
   }

   @Override
   public <T extends AsyncInterceptor> T findInterceptorWithClass(Class<T> interceptorClass) {
      List<AsyncInterceptor> localInterceptors = this.interceptors;
      for (AsyncInterceptor interceptor : localInterceptors) {
         if (interceptorMatches(interceptor, interceptorClass)) {
            return interceptorClass.cast(interceptor);
         }
      }
      return null;
   }

   public String toString() {
      StringBuilder sb = new StringBuilder();
      List<AsyncInterceptor> localInterceptors = this.interceptors;
      for (AsyncInterceptor interceptor : localInterceptors) {
         sb.append("\n\t>> ");
         sb.append(interceptor);
      }
      return sb.toString();
   }

   @Override
   public boolean containsInstance(AsyncInterceptor interceptor) {
      List<AsyncInterceptor> localInterceptors = this.interceptors;
      for (AsyncInterceptor current : localInterceptors) {
         if (current == interceptor) {
            return true;
         }
      }
      return false;
   }

   @Override
   public boolean containsInterceptorType(Class<? extends AsyncInterceptor> interceptorType) {
      return containsInterceptorType(interceptorType, false);
   }

   @Override
   public boolean containsInterceptorType(Class<? extends AsyncInterceptor> interceptorType,
                                          boolean alsoMatchSubClasses) {
      List<AsyncInterceptor> localInterceptors = this.interceptors;
      for (AsyncInterceptor interceptor : localInterceptors) {
         Class<? extends AsyncInterceptor> currentInterceptorType = interceptor.getClass();
         if (alsoMatchSubClasses) {
            if (interceptorType.isAssignableFrom(currentInterceptorType)) {
               return true;
            }
         } else {
            if (interceptorType == currentInterceptorType) {
               return true;
            }
         }
      }
      return false;
   }

   @Override
   public List<AsyncInterceptor> getInterceptors() {
      return interceptors;
   }

   private void rebuildInterceptors() {
      ListIterator<AsyncInterceptor> it = interceptors.listIterator(interceptors.size());
      // The CallInterceptor
      AsyncInterceptor nextInterceptor = it.previous();
      DDAsyncInterceptor nextGetKeyValue = null;

      ((BaseAsyncInterceptor) nextInterceptor).setNextGetKeyValueInterceptor(null);
      if (overridesGetCommand(nextInterceptor.getClass())) {
         nextGetKeyValue = (DDAsyncInterceptor) nextInterceptor;
      }

      while (it.hasPrevious()) {
         AsyncInterceptor interceptor = it.previous();
         interceptor.setNextInterceptor(nextInterceptor);
         ((BaseAsyncInterceptor) interceptor).setNextGetKeyValueInterceptor(nextGetKeyValue);
         if (overridesGetCommand(interceptor.getClass())) {
            nextGetKeyValue = (DDAsyncInterceptor) interceptor;
         }
         nextInterceptor = interceptor;
      }
      this.firstInterceptor = nextInterceptor;
      this.firstGetKeyValueInterceptor = nextGetKeyValue;

   }

   private static boolean overridesGetCommand(Class<?> clazz) {
      boolean overridesGetKeyValue = overridesVisitMethod(clazz, "visitGetKeyValueCommand", GetKeyValueCommand.class);
      boolean overridesGetCacheEntry = overridesVisitMethod(clazz, "visitGetCacheEntryCommand", GetCacheEntryCommand.class);
      if (overridesGetKeyValue != overridesGetCacheEntry) {
         throw new IllegalStateException(clazz.getName() + " must override both visitGetKeyValueCommand and " +
               "visitGetCacheEntryCommand, or neither. Found: visitGetKeyValueCommand=" + overridesGetKeyValue +
               ", visitGetCacheEntryCommand=" + overridesGetCacheEntry);
      }
      return overridesGetKeyValue;
   }

   private static boolean overridesVisitMethod(Class<?> clazz, String methodName, Class<?> commandClass) {
      try {
         java.lang.reflect.Method method = clazz.getMethod(methodName, InvocationContext.class, commandClass);
         return method.getDeclaringClass() != DDAsyncInterceptor.class && !method.isAnnotationPresent(Skip.class);
      } catch (NoSuchMethodException e) {
         return false;
      }
   }
}
