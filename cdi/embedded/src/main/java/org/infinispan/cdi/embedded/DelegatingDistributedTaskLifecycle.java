package org.infinispan.cdi.embedded;

import java.util.Collection;
import java.util.concurrent.Callable;

import javax.enterprise.context.spi.CreationalContext;
import javax.enterprise.inject.spi.AnnotatedType;
import javax.enterprise.inject.spi.BeanManager;
import javax.enterprise.inject.spi.InjectionTarget;

import org.infinispan.Cache;
import org.infinispan.cdi.common.util.CDIHelper;
import org.infinispan.distexec.RunnableAdapter;
import org.infinispan.distexec.spi.DistributedTaskLifecycle;

public class DelegatingDistributedTaskLifecycle implements DistributedTaskLifecycle {
   private final DistributedTaskLifecycle delegate;

   public DelegatingDistributedTaskLifecycle() {
      delegate = CDIHelper.isCDIAvailable() ? new CDIDistributedTaskLifecycle() : new NoCDIDistributedTaskLifecycle();
   }

   @Override
   public <T, K, V> void onPreExecute(Callable<T> task, Cache<K, V> inputDataCache, Collection<K> inputKeys) {
      delegate.onPreExecute(task, inputDataCache, inputKeys);
   }

   @Override
   public <T> void onPostExecute(Callable<T> task) {
      delegate.onPostExecute(task);
   }

   static class NoCDIDistributedTaskLifecycle implements DistributedTaskLifecycle {

      @Override
      public <T, K, V> void onPreExecute(Callable<T> task, Cache<K, V> inputDataCache, Collection<K> inputKeys) {
      }

      @Override
      public <T> void onPostExecute(Callable<T> task) {
      }
   }

   static class CDIDistributedTaskLifecycle implements DistributedTaskLifecycle {

      @Override
      public <T, K, V> void onPreExecute(Callable<T> task, Cache<K, V> inputDataCache, Collection<K> inputKeys) {
         BeanManager bm = CDIHelper.getBeanManager();
         if (bm == null)
            return;
         ContextInputCache.set(inputDataCache);
         ContextInputCache.setKeys(inputKeys);
         preInject(bm, task);
         if (task instanceof RunnableAdapter) {
            preInject(bm, ((RunnableAdapter) task).getTask());
         }
      }

      @Override
      public <T> void onPostExecute(Callable<T> task) {
         try {
            BeanManager bm = CDIHelper.getBeanManager();
            if (bm == null)
               return;
            postInject(bm, task);
            if (task instanceof RunnableAdapter) {
               postInject(bm, ((RunnableAdapter) task).getTask());
            }
         } finally {
            ContextInputCache.clean();
         }
      }

      private <T> void preInject(BeanManager bm, T task) {
         Class<T> clazz = (Class<T>) task.getClass();
         AnnotatedType<T> type = bm.createAnnotatedType(clazz);
         InjectionTarget<T> it = bm.createInjectionTarget(type);
         CreationalContext<T> ctx = bm.createCreationalContext(null);
         it.inject(task, ctx);
         it.postConstruct(task);
      }

      private <T> void postInject(BeanManager bm, T task) {
         Class<T> clazz = (Class<T>) task.getClass();
         AnnotatedType<T> type = bm.createAnnotatedType(clazz);
         InjectionTarget<T> it = bm.createInjectionTarget(type);
         it.preDestroy(task);
         it.dispose(task);
      }
   }
}
