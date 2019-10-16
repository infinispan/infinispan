package org.infinispan.jcache.annotation;

import static java.util.Collections.unmodifiableSet;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Set;

import javax.cache.annotation.CacheKeyGenerator;
import javax.cache.annotation.CacheMethodDetails;
import javax.cache.annotation.CacheResolver;

/**
 * Metadata associated to a method annotated with a cache annotation.
 *
 * @author Kevin Pollet &lt;kevin.pollet@serli.com&gt; (C) 2011 SERLI
 */
public class MethodMetaData<A extends Annotation> implements CacheMethodDetails<A> {

   private final Method method;
   private final Set<Annotation> annotations;
   private final A cacheAnnotation;
   private final String cacheName;
   private final AggregatedParameterMetaData aggregatedParameterMetaData;
   private final CacheKeyGenerator cacheKeyGenerator;
   private final CacheResolver cacheResolver;
   private final CacheResolver exceptionCacheResolver;

   public MethodMetaData(Method method,
                         AggregatedParameterMetaData aggregatedParameterMetaData,
                         Set<Annotation> annotations,
                         CacheKeyGenerator cacheKeyGenerator,
                         CacheResolver cacheResolver, CacheResolver exceptionCacheResolver,
                         A cacheAnnotation,
                         String cacheName) {

      this.method = method;
      this.aggregatedParameterMetaData = aggregatedParameterMetaData;
      this.annotations = unmodifiableSet(annotations);
      this.cacheKeyGenerator = cacheKeyGenerator;
      this.cacheResolver = cacheResolver;
      this.exceptionCacheResolver = exceptionCacheResolver;
      this.cacheAnnotation = cacheAnnotation;
      this.cacheName = cacheName;
   }

   public Method getMethod() {
      return method;
   }

   public Set<Annotation> getAnnotations() {
      return annotations;
   }

   public A getCacheAnnotation() {
      return cacheAnnotation;
   }

   public String getCacheName() {
      return cacheName;
   }

   public CacheKeyGenerator getCacheKeyGenerator() {
      return cacheKeyGenerator;
   }

   public CacheResolver getCacheResolver() {
      return cacheResolver;
   }

   public CacheResolver getExceptionCacheResolver() {
      return exceptionCacheResolver;
   }

   public List<ParameterMetaData> getParameters() {
      return aggregatedParameterMetaData.getParameters();
   }

   public List<ParameterMetaData> getKeyParameters() {
      return aggregatedParameterMetaData.getKeyParameters();
   }

   public ParameterMetaData getValueParameter() {
      return aggregatedParameterMetaData.getValueParameter();
   }

   @Override
   public String toString() {
      return new StringBuilder()
            .append("MethodMetaData{")
            .append("method=").append(method)
            .append(", annotations=").append(annotations)
            .append(", cacheAnnotation=").append(cacheAnnotation)
            .append(", cacheName='").append(cacheName).append('\'')
            .append(", aggregatedParameterMetaData=").append(aggregatedParameterMetaData)
            .append(", cacheKeyGenerator=").append(cacheKeyGenerator)
            .append('}')
            .toString();
   }
}
