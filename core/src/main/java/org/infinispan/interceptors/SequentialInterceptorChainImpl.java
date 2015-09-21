package org.infinispan.interceptors;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.ReflectionUtil;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.components.ComponentMetadataRepo;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.interceptors.base.AnyInterceptor;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.interceptors.base.SequentialInterceptor;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

/**
 * Knows how to build and manage an chain of interceptors. Also in charge with invoking methods on the chain.
 *
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @author Dan Berindei
 * @since 4.0
 */
@Scope(Scopes.NAMED_CACHE)
public class SequentialInterceptorChainImpl implements SequentialInterceptorChain {

   private static final Log log = LogFactory.getLog(SequentialInterceptorChainImpl.class);

   final ComponentMetadataRepo componentMetadataRepo;
   final ReentrantLock lock = new ReentrantLock();

   private final List<SequentialInterceptor> interceptors = new CopyOnWriteArrayList<>();

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

   private void validateCustomInterceptor(Class<? extends AnyInterceptor> i) {
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
   private void assertNotAdded(Class<? extends AnyInterceptor> clazz) {
      if (containsInterceptorType(clazz, false))
         throw new CacheConfigurationException("Detected interceptor of type [" + clazz.getName() +
                                                     "] being added to the interceptor chain " +
                                                     System.identityHashCode(this) + " more than once!");
   }

   public void addInterceptor(AnyInterceptor interceptor, int position) {
      final ReentrantLock lock = this.lock;
      lock.lock();
      try {
         Class<? extends AnyInterceptor> interceptorClass = interceptor.getClass();
         assertNotAdded(interceptorClass);
         validateCustomInterceptor(interceptorClass);
         interceptors.add(position, makeSequentialInterceptor(interceptor));
      } finally {
         lock.unlock();
      }
   }

   public void removeInterceptor(int position) {
      final ReentrantLock lock = this.lock;
      lock.lock();
      try {
         interceptors.remove(position);
      } finally {
         lock.unlock();
      }
   }

   public int size() {
      return interceptors.size();
   }

   public List<CommandInterceptor> asList() {
      return Collections.emptyList();
   }

   public void removeInterceptor(Class<? extends AnyInterceptor> clazz) {
      final ReentrantLock lock = this.lock;
      lock.lock();
      try {
         for (int i = 0; i < interceptors.size(); i++) {
            if (interceptorMatches(interceptors.get(i), clazz)) {
               interceptors.remove(i);
            }
         }
      } finally {
         lock.unlock();
      }
   }

   protected boolean interceptorMatches(SequentialInterceptor interceptor,
                                        Class<? extends AnyInterceptor> clazz) {
      return clazz == getRealInterceptorType(interceptor);
   }

   public boolean addInterceptorAfter(AnyInterceptor toAdd,
                                      Class<? extends AnyInterceptor> afterInterceptor) {
      lock.lock();
      try {
         Class<? extends AnyInterceptor> interceptorClass = toAdd.getClass();
         assertNotAdded(interceptorClass);
         validateCustomInterceptor(interceptorClass);
         for (int i = 0; i < interceptors.size(); i++) {
            if (interceptorMatches(interceptors.get(i), afterInterceptor)) {
               interceptors.add(i + 1, makeSequentialInterceptor(toAdd));
               return true;
            }
         }
         return false;
      } finally {
         lock.unlock();
      }
   }

   @Deprecated
   public boolean addInterceptorBefore(AnyInterceptor toAdd,
                                       Class<? extends AnyInterceptor> beforeInterceptor,
                                       boolean isCustom) {
      if (isCustom)
         validateCustomInterceptor(toAdd.getClass());
      return addInterceptorBefore(toAdd, beforeInterceptor);
   }

   public boolean addInterceptorBefore(AnyInterceptor toAdd,
                                       Class<? extends AnyInterceptor> beforeInterceptor) {
      lock.lock();
      try {
         Class<? extends AnyInterceptor> interceptorClass = toAdd.getClass();
         assertNotAdded(interceptorClass);
         validateCustomInterceptor(interceptorClass);
         for (int i = 0; i < interceptors.size(); i++) {
            if (interceptorMatches(interceptors.get(i), beforeInterceptor)) {
               interceptors.add(i, makeSequentialInterceptor(toAdd));
               return true;
            }
         }
         return false;
      } finally {
         lock.unlock();
      }
   }

   public boolean replaceInterceptor(AnyInterceptor replacingInterceptor,
                                     Class<? extends AnyInterceptor> toBeReplacedInterceptorType) {
      final ReentrantLock lock = this.lock;
      lock.lock();
      try {
         Class<? extends AnyInterceptor> interceptorClass = replacingInterceptor.getClass();
         assertNotAdded(interceptorClass);
         validateCustomInterceptor(interceptorClass);

         for (int i = 0; i < interceptors.size(); i++) {
            if (interceptorMatches(interceptors.get(i), toBeReplacedInterceptorType)) {
               interceptors.set(i, makeSequentialInterceptor(replacingInterceptor));
               return true;
            }
         }
         return false;
      } finally {
         lock.unlock();
      }
   }

   public void appendInterceptor(AnyInterceptor ci, boolean isCustom) {
      Class<? extends AnyInterceptor> interceptorClass = ci.getClass();
      if (isCustom)
         validateCustomInterceptor(interceptorClass);
      assertNotAdded(interceptorClass);
      // Called when building interceptor chain and so concurrent start calls are protected already
      interceptors.add(makeSequentialInterceptor(ci));
   }

   public Object invoke(InvocationContext ctx, VisitableCommand command) {
      try {
         CompletableFuture<Object> future = ctx.execute(command);
         return future.get();
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new CacheException(e);
      } catch (ExecutionException e) {
         if (e.getCause() instanceof RuntimeException) {
            throw (RuntimeException) e.getCause();
         } else {
            throw new CacheException(e.getCause());
         }
      }
   }

   public AnyInterceptor findInterceptorExtending(Class<? extends AnyInterceptor> interceptorClass) {
      for (SequentialInterceptor interceptor : interceptors) {
         AnyInterceptor realInterceptor = getRealInterceptor(interceptor);
         boolean isSubclass = interceptorClass.isInstance(realInterceptor);
         if (isSubclass) {
            return realInterceptor;
         }
      }
      return null;
   }

   @Override
   public AnyInterceptor findInterceptorWithClass(Class interceptorClass) {
      for (SequentialInterceptor interceptor : interceptors) {
         AnyInterceptor realInterceptor = getRealInterceptor(interceptor);
         if (realInterceptor.getClass() == interceptorClass) {
            return realInterceptor;
         }
      }
      return null;
   }

   public String toString() {
      StringBuilder sb = new StringBuilder();
      for (SequentialInterceptor interceptor : interceptors) {
         sb.append("\n\t>> ");
         sb.append(interceptor);
      }
      return sb.toString();
   }

   public boolean containsInstance(AnyInterceptor interceptor) {
      for (SequentialInterceptor current : interceptors) {
         if (getRealInterceptor(current) == interceptor) {
            return true;
         }
      }
      return false;
   }

   public boolean containsInterceptorType(Class<? extends AnyInterceptor> interceptorType) {
      return containsInterceptorType(interceptorType, false);
   }

   @Override
   public boolean containsInterceptorType(Class<? extends AnyInterceptor> interceptorType,
                                          boolean alsoMatchSubClasses) {
      for (SequentialInterceptor interceptor : interceptors) {
         Class<? extends AnyInterceptor> currentInterceptorType = getRealInterceptorType(interceptor);
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
   public List<SequentialInterceptor> getInterceptors() {
      // The new instance will not be affected by changes to the original list
      return new CopyOnWriteArrayList<>(interceptors);
   }

   @Override
   public Stream<AnyInterceptor> getRealInterceptors() {
      return getInterceptors().stream().map(this::getRealInterceptor);
   }

   protected SequentialInterceptor makeSequentialInterceptor(AnyInterceptor interceptor) {
      SequentialInterceptor theInterceptor;
      if (interceptor instanceof SequentialInterceptor) {
         theInterceptor = (SequentialInterceptor) interceptor;
      } else {
         theInterceptor = new SequentialInterceptorAdapter((CommandInterceptor) interceptor);
      }
      return theInterceptor;
   }

   protected AnyInterceptor getRealInterceptor(SequentialInterceptor interceptor) {
      if (interceptor instanceof SequentialInterceptorAdapter) {
         SequentialInterceptorAdapter adapter = (SequentialInterceptorAdapter) interceptor;
         return adapter.getAdaptedInterceptor();
      } else {
         return interceptor;
      }
   }

   protected Class<? extends AnyInterceptor> getRealInterceptorType(SequentialInterceptor interceptor) {
      if (interceptor instanceof SequentialInterceptorAdapter) {
         SequentialInterceptorAdapter adapter = (SequentialInterceptorAdapter) interceptor;
         return adapter.getAdaptedType();
      } else {
         return interceptor.getClass();
      }
   }
}
