package org.infinispan.cdi;

import java.util.concurrent.Callable;

import javax.enterprise.context.spi.CreationalContext;
import javax.enterprise.inject.spi.AnnotatedType;
import javax.enterprise.inject.spi.BeanManager;
import javax.enterprise.inject.spi.InjectionTarget;

import org.infinispan.distexec.RemoteExecutable;
import org.infinispan.distexec.spi.DistributedTaskLifecycle;
import org.jboss.solder.beanManager.BeanManagerAware;

public class CDIDistributedTaskLifecycle extends BeanManagerAware implements DistributedTaskLifecycle {
        
   @Override
   @SuppressWarnings({ "rawtypes", "unchecked" })
   public <T> void onPreExecute(Callable<T> task) {
      BeanManager bm = InfinispanExtension.getBeanManagerController().getRegisteredBeanManager();
      Class<Callable> clazz = (Class<Callable>) task.getClass();
      AnnotatedType<Callable> type = (AnnotatedType<Callable>) bm.createAnnotatedType(clazz);
      InjectionTarget<Callable> it = bm.createInjectionTarget(type);
      CreationalContext<Callable> ctx = bm.createCreationalContext(null);
      it.inject(task, ctx);
      it.postConstruct(task);
   }

   @Override
   @SuppressWarnings({ "rawtypes", "unchecked" })
   public <T> void onPostExecute(Callable<T> task) {            
      BeanManager bm = InfinispanExtension.getBeanManagerController().getRegisteredBeanManager();
      Class<Callable> clazz = (Class<Callable>) task.getClass();
      AnnotatedType<Callable> type = (AnnotatedType<Callable>) bm.createAnnotatedType(clazz);
      InjectionTarget<Callable> it = bm.createInjectionTarget(type);
      it.preDestroy(task);
      it.dispose(task);
   }

   @Override
   @SuppressWarnings("unchecked")
   public void onPreExecute(RemoteExecutable remoteExecutable) {      
      BeanManager bm = InfinispanExtension.getBeanManagerController().getRegisteredBeanManager();
      Class<RemoteExecutable> clazz = (Class<RemoteExecutable>) remoteExecutable.getClass();
      AnnotatedType<RemoteExecutable> type = (AnnotatedType<RemoteExecutable>) bm.createAnnotatedType(clazz);
      InjectionTarget<RemoteExecutable> it = bm.createInjectionTarget(type);
      CreationalContext<RemoteExecutable> ctx = bm.createCreationalContext(null);
      it.inject(remoteExecutable, ctx);
      it.postConstruct(remoteExecutable);

   }

   @Override
   @SuppressWarnings("unchecked")
   public void onPostExecute(RemoteExecutable remoteExecutable) {
      BeanManager bm = InfinispanExtension.getBeanManagerController().getRegisteredBeanManager();
      Class<RemoteExecutable> clazz = (Class<RemoteExecutable>) remoteExecutable.getClass();
      AnnotatedType<RemoteExecutable> type = (AnnotatedType<RemoteExecutable>) bm.createAnnotatedType(clazz);
      InjectionTarget<RemoteExecutable> it = bm.createInjectionTarget(type);
      it.preDestroy(remoteExecutable);
      it.dispose(remoteExecutable);
   }
}
