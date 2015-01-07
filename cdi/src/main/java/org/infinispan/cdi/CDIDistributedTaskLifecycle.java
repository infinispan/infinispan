package org.infinispan.cdi;

import java.util.concurrent.Callable;

import javax.enterprise.context.spi.CreationalContext;
import javax.enterprise.inject.spi.AnnotatedType;
import javax.enterprise.inject.spi.BeanManager;
import javax.enterprise.inject.spi.InjectionTarget;

import org.infinispan.Cache;
import org.infinispan.cdi.util.BeanManagerProvider;
import org.infinispan.distexec.spi.DistributedTaskLifecycle;

public class CDIDistributedTaskLifecycle implements
         DistributedTaskLifecycle {
   private final boolean HAVE_CDI;
   
   public CDIDistributedTaskLifecycle() {
      boolean success;
      try {
         this.getClass().getClassLoader().loadClass("javax.enterprise.inject.spi.BeanManager");
         success = true;
      } catch(ClassNotFoundException e) {
         success = false;
      }
      HAVE_CDI = success;
   }


   @Override
   @SuppressWarnings({ "unchecked" })
   public <T, K, V> void onPreExecute(Callable<T> task, Cache<K, V> inputCache) {
      if (HAVE_CDI) {
         BeanManager bm = BeanManagerProvider.getInstance().getBeanManager();
         ContextInputCache.set(inputCache);
         Class<Callable<T>> clazz = (Class<Callable<T>>) task.getClass();
         AnnotatedType<Callable<T>> type = bm.createAnnotatedType(clazz);
         InjectionTarget<Callable<T>> it = bm.createInjectionTarget(type);
         CreationalContext<Callable<T>> ctx = bm.createCreationalContext(null);
         it.inject(task, ctx);
         it.postConstruct(task);
      }
   }

   @Override
   @SuppressWarnings({ "unchecked" })
   public <T> void onPostExecute(Callable<T> task) {
      if (HAVE_CDI) {
         try {
            BeanManager bm = BeanManagerProvider.getInstance().getBeanManager();
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
