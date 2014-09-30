package org.infinispan.cdi;

import javax.enterprise.context.spi.CreationalContext;
import javax.enterprise.inject.spi.AnnotatedType;
import javax.enterprise.inject.spi.BeanManager;
import javax.enterprise.inject.spi.InjectionTarget;

import org.infinispan.Cache;
import org.infinispan.cdi.util.BeanManagerProvider;
import org.infinispan.distexec.mapreduce.Mapper;
import org.infinispan.distexec.mapreduce.Reducer;
import org.infinispan.distexec.mapreduce.spi.MapReduceTaskLifecycle;


public class CDIMapReduceTaskLifecycle implements MapReduceTaskLifecycle {
  
   @Override
   @SuppressWarnings({ "unchecked" })
   public <KIn, VIn, KOut, VOut> void onPreExecute(Mapper<KIn, VIn, KOut, VOut> mapper, Cache<KIn, VIn> inputCache) {
      BeanManager bm = BeanManagerProvider.getInstance().getBeanManager();
      ContextInputCache.set(inputCache);
      Class<Mapper<KIn, VIn, KOut, VOut>> clazz = (Class<Mapper<KIn, VIn, KOut, VOut>>) mapper.getClass();
      AnnotatedType<Mapper<KIn, VIn, KOut, VOut>> type = bm.createAnnotatedType(clazz);
      InjectionTarget<Mapper<KIn, VIn, KOut, VOut>> it = bm.createInjectionTarget(type);
      CreationalContext<Mapper<KIn, VIn, KOut, VOut>> ctx = bm.createCreationalContext(null);
      it.inject(mapper, ctx);
      it.postConstruct(mapper);
   }

   @Override
   @SuppressWarnings({ "unchecked" })
   public <KIn, VIn, KOut, VOut> void onPostExecute(Mapper<KIn, VIn, KOut, VOut> mapper) {
      try {
         BeanManager bm = BeanManagerProvider.getInstance().getBeanManager();
         Class<Mapper<KIn, VIn, KOut, VOut>> clazz = (Class<Mapper<KIn, VIn, KOut, VOut>>) mapper.getClass();
         AnnotatedType<Mapper<KIn, VIn, KOut, VOut>> type = bm.createAnnotatedType(clazz);
         InjectionTarget<Mapper<KIn, VIn, KOut, VOut>> it = bm.createInjectionTarget(type);
         it.preDestroy(mapper);
         it.dispose(mapper);
      } finally {
         ContextInputCache.clean();
      }
   }

   @Override
   @SuppressWarnings({ "unchecked" })
   public <KOut, VOut> void onPreExecute(Reducer<KOut, VOut> reducer, Cache<?,?> cache) {
      BeanManager bm = BeanManagerProvider.getInstance().getBeanManager();
      Class<Reducer<KOut, VOut>> clazz = (Class<Reducer<KOut, VOut>>) reducer.getClass();
      AnnotatedType<Reducer<KOut, VOut>> type = bm.createAnnotatedType(clazz);
      InjectionTarget<Reducer<KOut, VOut>> it = bm.createInjectionTarget(type);
      CreationalContext<Reducer<KOut, VOut>> ctx = bm.createCreationalContext(null);
      it.inject(reducer, ctx);
      it.postConstruct(reducer);
   }

   @Override
   @SuppressWarnings({ "unchecked" })
   public <KOut, VOut> void onPostExecute(Reducer<KOut, VOut> reducer) {
      BeanManager bm = BeanManagerProvider.getInstance().getBeanManager();
      Class<Reducer<KOut, VOut>> clazz = (Class<Reducer<KOut, VOut>>) reducer.getClass();
      AnnotatedType<Reducer<KOut, VOut>> type = bm.createAnnotatedType(clazz);
      InjectionTarget<Reducer<KOut, VOut>> it = bm.createInjectionTarget(type);
      it.preDestroy(reducer);
      it.dispose(reducer);
   }
}
