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
import org.infinispan.interceptors.base.BaseSequentialInterceptor;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.interceptors.base.SequentialInterceptor;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;

/**
 * Knows how to build and manage an chain of interceptors. Also in charge with invoking methods on the chain.
 *
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @since 4.0
 */
@Scope(Scopes.NAMED_CACHE)
public class SequentialInterceptorChainImpl extends InterceptorChain implements SequentialInterceptorChain {

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

   private void validateCustomInterceptor(Class<? extends CommandInterceptor> i) {
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
   private void assertNotAdded(Class<? extends CommandInterceptor> clazz) {
      if (containsInterceptorType(clazz))
         throw new CacheConfigurationException("Detected interceptor of type [" + clazz.getName() +
                                                     "] being added to the interceptor chain " +
                                                     System.identityHashCode(this) + " more than once!");
   }

   public void addInterceptor(CommandInterceptor interceptor, int position) {
      final ReentrantLock lock = this.lock;
      lock.lock();
      try {
         Class<? extends CommandInterceptor> interceptorClass = interceptor.getClass();
         assertNotAdded(interceptorClass);
         validateCustomInterceptor(interceptorClass);
         interceptors.add(position, new SequentialInterceptorAdapter(interceptor));
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
      ArrayList<CommandInterceptor> list = new ArrayList<>(interceptors.size());
      interceptors.forEach(interceptor -> {
         if (interceptor instanceof SequentialInterceptorAdapter) {
            list.add(((SequentialInterceptorAdapter) interceptor).getAdaptedInterceptor());
         }
      });
      return list;
   }

   public void removeInterceptor(Class<? extends CommandInterceptor> clazz) {
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
                                        Class<? extends CommandInterceptor> clazz) {
      if (interceptor instanceof SequentialInterceptorAdapter) {
         Class<? extends CommandInterceptor> adaptedType =
               ((SequentialInterceptorAdapter) interceptor).getAdaptedType();
         if (clazz == adaptedType) {
            return true;
         }
      }
      return false;
   }

   public boolean addInterceptorAfter(CommandInterceptor toAdd,
                                      Class<? extends CommandInterceptor> afterInterceptor) {
      lock.lock();
      try {
         Class<? extends CommandInterceptor> interceptorClass = toAdd.getClass();
         assertNotAdded(interceptorClass);
         validateCustomInterceptor(interceptorClass);
         for (int i = 0; i < interceptors.size(); i++) {
            if (interceptorMatches(interceptors.get(i), afterInterceptor)) {
               interceptors.add(i + 1, new SequentialInterceptorAdapter(toAdd));
               return true;
            }
         }
         return false;
      } finally {
         lock.unlock();
      }
   }

   @Deprecated
   public boolean addInterceptorBefore(CommandInterceptor toAdd,
                                       Class<? extends CommandInterceptor> beforeInterceptor,
                                       boolean isCustom) {
      if (isCustom)
         validateCustomInterceptor(toAdd.getClass());
      return addInterceptorBefore(toAdd, beforeInterceptor);
   }

   public boolean addInterceptorBefore(CommandInterceptor toAdd,
                                       Class<? extends CommandInterceptor> beforeInterceptor) {
      lock.lock();
      try {
         Class<? extends CommandInterceptor> interceptorClass = toAdd.getClass();
         assertNotAdded(interceptorClass);
         validateCustomInterceptor(interceptorClass);
         for (int i = 0; i < interceptors.size(); i++) {
            if (interceptorMatches(interceptors.get(i), beforeInterceptor)) {
               interceptors.add(i, new SequentialInterceptorAdapter(toAdd));
               return true;
            }
         }
         return false;
      } finally {
         lock.unlock();
      }
   }

   public boolean replaceInterceptor(CommandInterceptor replacingInterceptor,
                                     Class<? extends CommandInterceptor> toBeReplacedInterceptorType) {
      final ReentrantLock lock = this.lock;
      lock.lock();
      try {
         Class<? extends CommandInterceptor> interceptorClass = replacingInterceptor.getClass();
         assertNotAdded(interceptorClass);
         validateCustomInterceptor(interceptorClass);

         for (int i = 0; i < interceptors.size(); i++) {
            if (interceptorMatches(interceptors.get(i), toBeReplacedInterceptorType)) {
               interceptors.set(i, new SequentialInterceptorAdapter(replacingInterceptor));
               return true;
            }
         }
         return false;
      } finally {
         lock.unlock();
      }
   }

   public void appendInterceptor(CommandInterceptor ci, boolean isCustom) {
      Class<? extends CommandInterceptor> interceptorClass = ci.getClass();
      if (isCustom)
         validateCustomInterceptor(interceptorClass);
      assertNotAdded(interceptorClass);
      // Called when building interceptor chain and so concurrent start calls are protected already
      interceptors.add(new SequentialInterceptorAdapter(ci));
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

   public CommandInterceptor getFirstInChain() {
      throw new UnsupportedOperationException();
   }

   public void setFirstInChain(CommandInterceptor interceptor) {
      throw new UnsupportedOperationException();
   }

   public List<CommandInterceptor> getInterceptorsWhichExtend(
         Class<? extends CommandInterceptor> interceptorClass) {
      List<CommandInterceptor> result = new LinkedList<>();
      for (SequentialInterceptor interceptor : interceptors) {
         if (interceptor instanceof SequentialInterceptorAdapter) {
            SequentialInterceptorAdapter adapter = (SequentialInterceptorAdapter) interceptor;
            Class<? extends CommandInterceptor> adaptedType = adapter.getAdaptedType();
            boolean isSubclass = interceptorClass.isAssignableFrom(adaptedType);
            if (isSubclass) {
               result.add(adapter.getAdaptedInterceptor());
            }
         }
      }
      return result;
   }

   public List<CommandInterceptor> getInterceptorsWithClass(Class clazz) {
      List<CommandInterceptor> result = new LinkedList<>();
      for (SequentialInterceptor interceptor : interceptors) {
         if (interceptorMatches(interceptor, clazz)) {
            SequentialInterceptorAdapter adapter = (SequentialInterceptorAdapter) interceptor;
            result.add(adapter.getAdaptedInterceptor());
         }
      }
      return result;
   }

   public String toString() {
      StringBuilder sb = new StringBuilder();
      for (SequentialInterceptor interceptor : interceptors) {
         sb.append("\n\t>> ");
         sb.append(interceptor);
      }
      return sb.toString();
   }

   public boolean containsInstance(CommandInterceptor interceptor) {
      for (SequentialInterceptor current : interceptors) {
         if (current instanceof SequentialInterceptorAdapter) {
            SequentialInterceptorAdapter adapter = (SequentialInterceptorAdapter) current;
            if (adapter.getAdaptedInterceptor() == interceptor) {
               return true;
            }
         }
      }
      return false;
   }

   public boolean containsInterceptorType(Class<? extends CommandInterceptor> interceptorType) {
      return containsInterceptorType(interceptorType, false);
   }

   public boolean containsInterceptorType(Class<? extends CommandInterceptor> interceptorType,
                                          boolean alsoMatchSubClasses) {
      for (SequentialInterceptor interceptor : interceptors) {
         if (interceptor instanceof SequentialInterceptorAdapter) {
            SequentialInterceptorAdapter adapter = (SequentialInterceptorAdapter) interceptor;
            Class<? extends CommandInterceptor> adaptedType = adapter.getAdaptedType();
            if (alsoMatchSubClasses) {
               if (interceptorType.isAssignableFrom(adaptedType)) {
                  return true;
               }
            } else {
               if (interceptorType == adaptedType) {
                  return true;
               }
            }
         }
      }
      return false;
   }

   @Override
   public List<SequentialInterceptor> getInterceptors() {
      return new CopyOnWriteArrayList<>(interceptors);
   }
}
