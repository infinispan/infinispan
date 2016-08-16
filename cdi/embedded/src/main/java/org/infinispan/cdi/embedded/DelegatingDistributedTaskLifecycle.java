package org.infinispan.cdi.embedded;

import java.util.concurrent.Callable;

import javax.enterprise.context.spi.CreationalContext;
import javax.enterprise.inject.spi.AnnotatedType;
import javax.enterprise.inject.spi.BeanManager;
import javax.enterprise.inject.spi.InjectionTarget;

import org.infinispan.Cache;
import org.infinispan.cdi.common.util.CDIHelper;
import org.infinispan.distexec.spi.DistributedTaskLifecycle;

public class DelegatingDistributedTaskLifecycle implements DistributedTaskLifecycle {
   private final DistributedTaskLifecycle delegate;

   public DelegatingDistributedTaskLifecycle() {
      delegate = CDIHelper.isCDIAvailable() ? new CDIDistributedTaskLifecycle() : new NoCDIDistributedTaskLifecycle();
   }

   @Override
   public <T, K, V> void onPreExecute(Callable<T> task, Cache<K, V> inputDataCache) {
      delegate.onPreExecute(task, inputDataCache);
   }

   @Override
   public <T> void onPostExecute(Callable<T> task) {
      delegate.onPostExecute(task);
   }

   static class NoCDIDistributedTaskLifecycle implements DistributedTaskLifecycle {

      @Override
      public <T, K, V> void onPreExecute(Callable<T> task, Cache<K, V> inputDataCache) {
      }

      @Override
      public <T> void onPostExecute(Callable<T> task) {
      }
   }

   static class CDIDistributedTaskLifecycle implements DistributedTaskLifecycle {

      @Override
      public <T, K, V> void onPreExecute(Callable<T> task, Cache<K, V> inputDataCache) {
         BeanManager bm = CDIHelper.getBeanManager();
         if (bm == null)
            return;
         ContextInputCache.set(inputDataCache);
         Class<Callable<T>> clazz = (Class<Callable<T>>) task.getClass();
         AnnotatedType<Callable<T>> type = bm.createAnnotatedType(clazz);
         InjectionTarget<Callable<T>> it = bm.createInjectionTarget(type);
         CreationalContext<Callable<T>> ctx = bm.createCreationalContext(null);
         it.inject(task, ctx);
         it.postConstruct(task);
      }

      @Override
      public <T> void onPostExecute(Callable<T> task) {
         try {
            BeanManager bm = CDIHelper.getBeanManager();
            if (bm == null)
               return;
            Class<Callable<T>> clazz = (Class<Callable<T>>) task.getClass();
            AnnotatedType<Callable<T>> type = bm.createAnnotatedType(clazz);
            InjectionTarget<Callable<T>> it = bm.createInjectionTarget(type);
            it.preDestroy(task);
            it.dispose(task);
         } finally {
            ContextInputCache.clean();
         }
      }
   }
}
