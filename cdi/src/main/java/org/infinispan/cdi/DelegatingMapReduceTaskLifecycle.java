package org.infinispan.cdi;

import javax.enterprise.context.spi.CreationalContext;
import javax.enterprise.inject.spi.AnnotatedType;
import javax.enterprise.inject.spi.BeanManager;
import javax.enterprise.inject.spi.InjectionTarget;

import org.infinispan.Cache;
import org.infinispan.cdi.util.CDIHelper;
import org.infinispan.distexec.mapreduce.Mapper;
import org.infinispan.distexec.mapreduce.Reducer;
import org.infinispan.distexec.mapreduce.spi.MapReduceTaskLifecycle;
import org.kohsuke.MetaInfServices;

@MetaInfServices
public class DelegatingMapReduceTaskLifecycle implements MapReduceTaskLifecycle {

   private final MapReduceTaskLifecycle delegate;

   public DelegatingMapReduceTaskLifecycle() {
      delegate = CDIHelper.isCDIAvailable() ? new CDIMapReduceTaskLifecycle() : new NoCDIMapReduceTaskLifecycle();
   }

   @Override
   public <KIn, VIn, KOut, VOut> void onPreExecute(Mapper<KIn, VIn, KOut, VOut> mapper, Cache<KIn, VIn> inputCache) {
      delegate.onPreExecute(mapper, inputCache);
   }

   @Override
   public <KIn, VIn, KOut, VOut> void onPostExecute(Mapper<KIn, VIn, KOut, VOut> mapper) {
      delegate.onPostExecute(mapper);
   }

   @Override
   public <KOut, VOut> void onPreExecute(Reducer<KOut, VOut> reducer, Cache<?, ?> cache) {
      delegate.onPreExecute(reducer, cache);
   }

   @Override
   public <KOut, VOut> void onPostExecute(Reducer<KOut, VOut> reducer) {
      delegate.onPostExecute(reducer);
   }

   static class NoCDIMapReduceTaskLifecycle implements MapReduceTaskLifecycle {

      @Override
      public <KIn, VIn, KOut, VOut> void onPreExecute(Mapper<KIn, VIn, KOut, VOut> mapper, Cache<KIn, VIn> inputCache) {
      }

      @Override
      public <KIn, VIn, KOut, VOut> void onPostExecute(Mapper<KIn, VIn, KOut, VOut> mapper) {
      }

      @Override
      public <KOut, VOut> void onPreExecute(Reducer<KOut, VOut> reducer, Cache<?, ?> inputCache) {
      }

      @Override
      public <KOut, VOut> void onPostExecute(Reducer<KOut, VOut> reducer) {
      }

   }

   static class CDIMapReduceTaskLifecycle implements MapReduceTaskLifecycle {

      @Override
      @SuppressWarnings("unchecked")
      public <KIn, VIn, KOut, VOut> void onPreExecute(Mapper<KIn, VIn, KOut, VOut> mapper, Cache<KIn, VIn> inputCache) {
         BeanManager bm = CDIHelper.getBeanManager();
         if (bm == null)
            return;
         ContextInputCache.set(inputCache);
         Class<Mapper<KIn, VIn, KOut, VOut>> clazz = (Class<Mapper<KIn, VIn, KOut, VOut>>) mapper.getClass();
         AnnotatedType<Mapper<KIn, VIn, KOut, VOut>> type = bm.createAnnotatedType(clazz);
         InjectionTarget<Mapper<KIn, VIn, KOut, VOut>> it = bm.createInjectionTarget(type);
         CreationalContext<Mapper<KIn, VIn, KOut, VOut>> ctx = bm.createCreationalContext(null);
         it.inject(mapper, ctx);
         it.postConstruct(mapper);
      }

      @Override
      @SuppressWarnings("unchecked")
      public <KIn, VIn, KOut, VOut> void onPostExecute(Mapper<KIn, VIn, KOut, VOut> mapper) {
         try {
            BeanManager bm = CDIHelper.getBeanManager();
            if (bm == null)
               return;
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
      @SuppressWarnings("unchecked")
      public <KOut, VOut> void onPreExecute(Reducer<KOut, VOut> reducer, Cache<?, ?> inputCache) {
         BeanManager bm = CDIHelper.getBeanManager();
         if (bm == null)
            return;
         Class<Reducer<KOut, VOut>> clazz = (Class<Reducer<KOut, VOut>>) reducer.getClass();
         AnnotatedType<Reducer<KOut, VOut>> type = bm.createAnnotatedType(clazz);
         InjectionTarget<Reducer<KOut, VOut>> it = bm.createInjectionTarget(type);
         CreationalContext<Reducer<KOut, VOut>> ctx = bm.createCreationalContext(null);
         it.inject(reducer, ctx);
         it.postConstruct(reducer);
      }

      @Override
      @SuppressWarnings("unchecked")
      public <KOut, VOut> void onPostExecute(Reducer<KOut, VOut> reducer) {
         BeanManager bm = CDIHelper.getBeanManager();
         if (bm == null)
            return;
         Class<Reducer<KOut, VOut>> clazz = (Class<Reducer<KOut, VOut>>) reducer.getClass();
         AnnotatedType<Reducer<KOut, VOut>> type = bm.createAnnotatedType(clazz);
         InjectionTarget<Reducer<KOut, VOut>> it = bm.createInjectionTarget(type);
         it.preDestroy(reducer);
         it.dispose(reducer);
      }

   }

}
