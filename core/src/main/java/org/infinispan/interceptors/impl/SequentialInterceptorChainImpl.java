package org.infinispan.interceptors.impl;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.ImmutableListCopy;
import org.infinispan.commons.util.ReflectionUtil;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.components.ComponentMetadataRepo;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.interceptors.SequentialInterceptor;
import org.infinispan.interceptors.SequentialInterceptorChain;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.IdentityHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;

import static org.infinispan.commons.util.Immutables.immutableListAdd;
import static org.infinispan.commons.util.Immutables.immutableListRemove;
import static org.infinispan.commons.util.Immutables.immutableListReplace;

/**
 * Knows how to build and manage an chain of interceptors. Also in charge with invoking methods on the chain.
 *
 * @author Dan Berindei
 * @since 9.0
 */
@Scope(Scopes.NAMED_CACHE)
@SuppressWarnings("deprecation")
public class SequentialInterceptorChainImpl implements SequentialInterceptorChain {
   // Using the same list type everywhere may help with the optimization of the invocation context methods
   private static final ImmutableListCopy<SequentialInterceptor> EMPTY_INTERCEPTORS_LIST =
         new ImmutableListCopy<>(new SequentialInterceptor[0]);
   private static final Map<Class<? extends CommandInterceptor>, Class<? extends SequentialInterceptor>>
         replacementInterceptors = new IdentityHashMap<>();

   static {
      // Populate the replacementInterceptors map
   }

   private static final Log log = LogFactory.getLog(SequentialInterceptorChainImpl.class);

   final ComponentMetadataRepo componentMetadataRepo;

   final ReentrantLock lock = new ReentrantLock();

   // Modifications are guarded with "lock", but reads do not need synchronization
   private volatile List<SequentialInterceptor> interceptors = EMPTY_INTERCEPTORS_LIST;
   private volatile InterceptorListNode firstInterceptor = null;

   public SequentialInterceptorChainImpl(ComponentMetadataRepo componentMetadataRepo) {
      this.componentMetadataRepo = componentMetadataRepo;
   }

   @Start
   private void printChainInfo() {
      if (log.isDebugEnabled()) {
         log.debugf("Interceptor chain size: %d", size());
         log.debugf("Interceptor chain is: %s", toString());
      }
   }

   private void validateCustomInterceptor(Class<? extends SequentialInterceptor> i) {
      if ((!ReflectionUtil.getAllMethodsShallow(i, Inject.class).isEmpty() ||
            !ReflectionUtil.getAllMethodsShallow(i, Start.class).isEmpty() ||
            !ReflectionUtil.getAllMethodsShallow(i, Stop.class).isEmpty()) &&
            componentMetadataRepo.findComponentMetadata(i.getName()) == null) {
         log.customInterceptorExpectsInjection(i.getName());
      }
   }

   /**
    * Ensures that the interceptor of type passed in isn't already added
    *
    * @param clazz type of interceptor to check for
    */
   private void checkInterceptor(Class<? extends SequentialInterceptor> clazz) {
      if (replacementInterceptors.containsKey(clazz)) {
         throw new IllegalArgumentException("Cannot add deprecated interceptor " + clazz);
      }
      if (containsInterceptorType(clazz, false))
         throw new CacheConfigurationException("Detected interceptor of type [" + clazz.getName() +
                                                     "] being added to the interceptor chain " +
                                                     System.identityHashCode(this) + " more than once!");
   }

   public void addInterceptor(SequentialInterceptor interceptor, int position) {
      final ReentrantLock lock = this.lock;
      lock.lock();
      try {
         Class<? extends SequentialInterceptor> interceptorClass = interceptor.getClass();
         checkInterceptor(interceptorClass);
         validateCustomInterceptor(interceptorClass);
         interceptors = immutableListAdd(interceptors, position, interceptor);
         rebuildInterceptors();
      } finally {
         lock.unlock();
      }
   }

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

   public int size() {
      return interceptors.size();
   }

   public void removeInterceptor(Class<? extends SequentialInterceptor> clazz) {
      final ReentrantLock lock = this.lock;
      lock.lock();
      try {
         for (int i = 0; i < interceptors.size(); i++) {
            if (interceptorMatches(interceptors.get(i), getReplacementInterceptor(clazz))) {
               removeInterceptor(i);
               break;
            }
         }
      } finally {
         lock.unlock();
      }
   }

   protected boolean interceptorMatches(SequentialInterceptor interceptor,
                                        Class<? extends SequentialInterceptor> clazz) {
      Class<? extends SequentialInterceptor> interceptorType = interceptor.getClass();
      return clazz == interceptorType;
   }

   public boolean addInterceptorAfter(SequentialInterceptor toAdd,
                                      Class<? extends SequentialInterceptor> afterInterceptor) {
      lock.lock();
      try {
         Class<? extends SequentialInterceptor> interceptorClass = toAdd.getClass();
         checkInterceptor(interceptorClass);
         validateCustomInterceptor(interceptorClass);
         for (int i = 0; i < interceptors.size(); i++) {
            if (interceptorMatches(interceptors.get(i), getReplacementInterceptor(afterInterceptor))) {
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

   @Deprecated
   public boolean addInterceptorBefore(SequentialInterceptor toAdd,
                                       Class<? extends SequentialInterceptor> beforeInterceptor,
                                       boolean isCustom) {
      if (isCustom)
         validateCustomInterceptor(toAdd.getClass());
      return addInterceptorBefore(toAdd, beforeInterceptor);
   }

   public boolean addInterceptorBefore(SequentialInterceptor toAdd,
                                       Class<? extends SequentialInterceptor> beforeInterceptor) {
      lock.lock();
      try {
         Class<? extends SequentialInterceptor> interceptorClass = toAdd.getClass();
         checkInterceptor(interceptorClass);
         validateCustomInterceptor(interceptorClass);
         for (int i = 0; i < interceptors.size(); i++) {
            if (interceptorMatches(interceptors.get(i), getReplacementInterceptor(beforeInterceptor))) {
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

   public boolean replaceInterceptor(SequentialInterceptor replacingInterceptor,
                                     Class<? extends SequentialInterceptor> existingInterceptorType) {
      final ReentrantLock lock = this.lock;
      lock.lock();
      try {
         Class<? extends SequentialInterceptor> interceptorClass = replacingInterceptor.getClass();
         checkInterceptor(interceptorClass);
         validateCustomInterceptor(interceptorClass);

         for (int i = 0; i < interceptors.size(); i++) {
            if (interceptorMatches(interceptors.get(i), getReplacementInterceptor(existingInterceptorType))) {
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

   public void appendInterceptor(SequentialInterceptor ci, boolean isCustom) {
      lock.lock();
      try {
         Class<? extends SequentialInterceptor> interceptorClass = ci.getClass();
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

   public CompletableFuture<Object> invokeAsync(InvocationContext ctx, VisitableCommand command) {
      return ((BaseSequentialInvocationContext) ctx).invoke(command, firstInterceptor);
   }

   @Override
   public Object invoke(InvocationContext ctx, VisitableCommand command) {
      try {
         return ((BaseSequentialInvocationContext) ctx).invokeSync(command, firstInterceptor);
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new CacheException(e);
      } catch (RuntimeException e) {
         throw e;
      } catch (Throwable throwable) {
         throw new CacheException(throwable);
      }
   }

   public <T extends SequentialInterceptor> T findInterceptorExtending(Class<T> interceptorClass) {
      List<SequentialInterceptor> localInterceptors = this.interceptors;
      for (SequentialInterceptor interceptor : localInterceptors) {
         boolean isSubclass = interceptorClass.isInstance(interceptor);
         if (isSubclass) {
            return interceptorClass.cast(interceptor);
         }
      }
      return null;
   }

   @Override
   public <T extends SequentialInterceptor> T findInterceptorWithClass(Class<T> interceptorClass) {
      List<SequentialInterceptor> localInterceptors = this.interceptors;
      for (SequentialInterceptor interceptor : localInterceptors) {
         if (interceptorMatches(interceptor, interceptorClass)) {
            return interceptorClass.cast(interceptor);
         }
      }
      return null;
   }

   public String toString() {
      StringBuilder sb = new StringBuilder();
      List<SequentialInterceptor> localInterceptors = this.interceptors;
      for (SequentialInterceptor interceptor : localInterceptors) {
         sb.append("\n\t>> ");
         sb.append(interceptor);
      }
      return sb.toString();
   }

   public boolean containsInstance(SequentialInterceptor interceptor) {
      List<SequentialInterceptor> localInterceptors = this.interceptors;
      for (SequentialInterceptor current : localInterceptors) {
         if (current == interceptor) {
            return true;
         }
      }
      return false;
   }

   public boolean containsInterceptorType(Class<? extends SequentialInterceptor> interceptorType) {
      return containsInterceptorType(interceptorType, false);
   }

   @Override
   public boolean containsInterceptorType(Class<? extends SequentialInterceptor> interceptorType,
                                          boolean alsoMatchSubClasses) {
      List<SequentialInterceptor> localInterceptors = this.interceptors;
      for (SequentialInterceptor interceptor : localInterceptors) {
         Class<? extends SequentialInterceptor> currentInterceptorType = interceptor.getClass();
         if (alsoMatchSubClasses) {
            if (getReplacementInterceptor(interceptorType).isAssignableFrom(currentInterceptorType)) {
               return true;
            }
         } else {
            if (getReplacementInterceptor(interceptorType) == currentInterceptorType) {
               return true;
            }
         }
      }
      return false;
   }

   @Override
   public List<SequentialInterceptor> getInterceptors() {
      return interceptors;
   }

   private void rebuildInterceptors() {
      this.firstInterceptor = null;
      ListIterator<SequentialInterceptor> it = interceptors.listIterator(interceptors.size());
      while (it.hasPrevious()) {
         firstInterceptor = new InterceptorListNode(it.previous(), firstInterceptor);
      }
   }


   static Class<? extends SequentialInterceptor> getReplacementInterceptor(Class<? extends SequentialInterceptor> oldInterceptor) {
      return replacementInterceptors.getOrDefault(oldInterceptor, oldInterceptor);
   }
}
