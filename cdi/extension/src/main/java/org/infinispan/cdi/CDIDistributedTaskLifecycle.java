package org.infinispan.cdi;

import java.util.concurrent.Callable;

import javax.enterprise.context.spi.CreationalContext;
import javax.enterprise.inject.spi.AnnotatedType;
import javax.enterprise.inject.spi.BeanManager;
import javax.enterprise.inject.spi.InjectionTarget;

import org.infinispan.Cache;
import org.infinispan.distexec.spi.DistributedTaskLifecycle;
import org.jboss.solder.beanManager.BeanManagerAware;

public class CDIDistributedTaskLifecycle extends BeanManagerAware implements
         DistributedTaskLifecycle {


   @Override
   @SuppressWarnings({ "unchecked" })
   public <T, K, V> void onPreExecute(Callable<T> task, Cache<K, V> inputCache) {
      BeanManager bm = InfinispanExtension.getBeanManagerController().getRegisteredBeanManager();
      ContextInputCache.set(inputCache);
      Class<Callable<T>> clazz = (Class<Callable<T>>) task.getClass();
      AnnotatedType<Callable<T>> type = bm.createAnnotatedType(clazz);
      InjectionTarget<Callable<T>> it = bm.createInjectionTarget(type);
      CreationalContext<Callable<T>> ctx = bm.createCreationalContext(null);
      it.inject(task, ctx);
      it.postConstruct(task);    
   }

   @Override
   @SuppressWarnings({ "unchecked" })
   public <T> void onPostExecute(Callable<T> task) {
      try {
         BeanManager bm = InfinispanExtension.getBeanManagerController().getRegisteredBeanManager();
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
