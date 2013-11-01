package org.infinispan.jcache.annotation;

import org.infinispan.jcache.logging.Log;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.util.logging.LogFactory;

import javax.cache.annotation.CacheDefaults;
import javax.cache.annotation.CacheKey;
import javax.cache.annotation.CacheKeyGenerator;
import javax.cache.annotation.CacheKeyInvocationContext;
import javax.cache.annotation.CachePut;
import javax.cache.annotation.CacheRemoveAll;
import javax.cache.annotation.CacheRemove;
import javax.cache.annotation.CacheResult;
import javax.cache.annotation.CacheValue;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.spi.BeanManager;
import javax.inject.Inject;
import javax.interceptor.InvocationContext;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import static org.infinispan.jcache.annotation.CacheLookupHelper.getCacheKeyGenerator;
import static org.infinispan.jcache.annotation.CacheLookupHelper.getCacheName;
import static org.infinispan.jcache.annotation.CollectionsHelper.asSet;
import static org.infinispan.jcache.annotation.Contracts.assertNotNull;

/**
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 * @author Galder Zamarreño
 */
@ApplicationScoped
public class CacheKeyInvocationContextFactory {

   private static final Log log = LogFactory.getLog(CacheKeyInvocationContextFactory.class, Log.class);

   private BeanManager beanManager;
   private ConcurrentMap<Method, MethodMetaData<? extends Annotation>> methodMetaDataCache;

   @Inject
   public CacheKeyInvocationContextFactory(BeanManager beanManager) {
      this.beanManager = beanManager;
      this.methodMetaDataCache = CollectionFactory.makeConcurrentMap();
   }

   // for proxy.
   protected CacheKeyInvocationContextFactory() {
   }

   /**
    * Returns the cache key invocation context corresponding to the given invocation context.
    *
    * @param invocationContext the {@link javax.interceptor.InvocationContext}.
    * @return an instance of {@link javax.cache.annotation.CacheKeyInvocationContext} corresponding to the given {@link javax.interceptor.InvocationContext}.
    */
   public <A extends Annotation> CacheKeyInvocationContext<A> getCacheKeyInvocationContext(InvocationContext invocationContext) {
      assertNotNull(invocationContext, "invocationContext parameter must not be null");

      final MethodMetaData<A> methodMetaData = (MethodMetaData<A>) getMethodMetaData(invocationContext.getMethod());
      return new CacheKeyInvocationContextImpl<A>(invocationContext, methodMetaData);
   }

   /**
    * Returns the method meta data for the given method.
    *
    * @param method the method.
    * @return an instance of {@link MethodMetaData}.
    */
   private MethodMetaData<? extends Annotation> getMethodMetaData(Method method) {
      MethodMetaData<? extends Annotation> methodMetaData = methodMetaDataCache.get(method);

      if (methodMetaData == null) {
         final String cacheName;
         final Annotation cacheAnnotation;
         final AggregatedParameterMetaData aggregatedParameterMetaData;
         final CacheKeyGenerator cacheKeyGenerator;
         final CacheDefaults cacheDefaultsAnnotation = method.getDeclaringClass().getAnnotation(CacheDefaults.class);

         if (method.isAnnotationPresent(CacheResult.class)) {
            final CacheResult cacheResultAnnotation = method.getAnnotation(CacheResult.class);
            cacheKeyGenerator = getCacheKeyGenerator(beanManager, cacheResultAnnotation.cacheKeyGenerator(), cacheDefaultsAnnotation);
            cacheName = getCacheName(method, cacheResultAnnotation.cacheName(), cacheDefaultsAnnotation, true);
            aggregatedParameterMetaData = getAggregatedParameterMetaData(method, false);
            cacheAnnotation = cacheResultAnnotation;

         } else if (method.isAnnotationPresent(CacheRemove.class)) {
            final CacheRemove cacheRemoveEntryAnnotation = method.getAnnotation(CacheRemove.class);
            cacheKeyGenerator = getCacheKeyGenerator(beanManager, cacheRemoveEntryAnnotation.cacheKeyGenerator(), cacheDefaultsAnnotation);
            cacheName = getCacheName(method, cacheRemoveEntryAnnotation.cacheName(), cacheDefaultsAnnotation, false);
            aggregatedParameterMetaData = getAggregatedParameterMetaData(method, false);
            cacheAnnotation = cacheRemoveEntryAnnotation;

            if (cacheName.isEmpty()) {
               throw log.cacheRemoveEntryMethodWithoutCacheName(method.getName());
            }

         } else if (method.isAnnotationPresent(CacheRemoveAll.class)) {
            final CacheRemoveAll cacheRemoveAllAnnotation = method.getAnnotation(CacheRemoveAll.class);
            cacheKeyGenerator = null;
            cacheName = getCacheName(method, cacheRemoveAllAnnotation.cacheName(), cacheDefaultsAnnotation, false);
            aggregatedParameterMetaData = getAggregatedParameterMetaData(method, false);
            cacheAnnotation = cacheRemoveAllAnnotation;

            if (cacheName.isEmpty()) {
               throw log.cacheRemoveAllMethodWithoutCacheName(method.getName());
            }

         } else if (method.isAnnotationPresent(CachePut.class)) {
            final CachePut cachePutAnnotation = method.getAnnotation(CachePut.class);
            cacheKeyGenerator = getCacheKeyGenerator(beanManager, cachePutAnnotation.cacheKeyGenerator(), cacheDefaultsAnnotation);
            cacheName = getCacheName(method, cachePutAnnotation.cacheName(), cacheDefaultsAnnotation, true);
            aggregatedParameterMetaData = getAggregatedParameterMetaData(method, true);
            cacheAnnotation = cachePutAnnotation;

         } else {
            throw log.methodWithoutCacheAnnotation(method.getName());
         }

         final MethodMetaData<? extends Annotation> newCacheMethodMetaData = new MethodMetaData<Annotation>(
               method,
               aggregatedParameterMetaData,
               asSet(method.getAnnotations()),
               cacheKeyGenerator,
               cacheAnnotation,
               cacheName
         );

         methodMetaData = methodMetaDataCache.putIfAbsent(method, newCacheMethodMetaData);
         if (methodMetaData == null) {
            methodMetaData = newCacheMethodMetaData;
         }
      }

      return methodMetaData;
   }

   /**
    * Returns the aggregated parameter meta data for the given method.
    *
    * @param method            the method.
    * @param cacheValueAllowed {@code true} if the {@link javax.cache.annotation.CacheValue} annotation is allowed on a method parameter.
    * @return an instance of {@link AggregatedParameterMetaData}.
    */
   private AggregatedParameterMetaData getAggregatedParameterMetaData(Method method, boolean cacheValueAllowed) {
      final Class<?>[] parameterTypes = method.getParameterTypes();
      final Annotation[][] parameterAnnotations = method.getParameterAnnotations();
      final List<ParameterMetaData> parameters = new ArrayList<ParameterMetaData>();
      final List<ParameterMetaData> keyParameters = new ArrayList<ParameterMetaData>();
      ParameterMetaData valueParameter = null;

      for (int i = 0; i < parameterTypes.length; i++) {
         final Set<Annotation> annotations = asSet(parameterAnnotations[i]);
         final ParameterMetaData parameterMetaData = new ParameterMetaData(parameterTypes[i], i, annotations);

         for (Annotation oneAnnotation : annotations) {
            final Class<?> type = oneAnnotation.annotationType();

            if (CacheKey.class.equals(type)) {
               keyParameters.add(parameterMetaData);

            } else if (cacheValueAllowed && CacheValue.class.equals(type)) {
               if (valueParameter != null) {
                  throw log.cachePutMethodWithMoreThanOneCacheValueParameter(method.getName());
               }
               valueParameter = parameterMetaData;
            }
         }

         parameters.add(parameterMetaData);
      }

      if (cacheValueAllowed && valueParameter == null) {
         if (parameters.size() > 1) {
            throw log.cachePutMethodWithoutCacheValueParameter(method.getName());
         }
         valueParameter = parameters.get(0);
      }

      if (keyParameters.isEmpty()) {
         keyParameters.addAll(parameters);
      }

      if (valueParameter != null && keyParameters.size() > 1) {
         keyParameters.remove(valueParameter);
      }

      return new AggregatedParameterMetaData(parameters, keyParameters, valueParameter);
   }
}
