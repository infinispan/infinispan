package org.infinispan.jcache.annotation;

import static java.util.Collections.unmodifiableSet;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Set;

import javax.cache.annotation.CacheKeyGenerator;

/**
 * Metadata associated to a method annotated with a cache annotation.
 *
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
public class MethodMetaData<A extends Annotation> {

   private final Method method;
   private final Set<Annotation> annotations;
   private final A cacheAnnotation;
   private final String cacheName;
   private final AggregatedParameterMetaData aggregatedParameterMetaData;
   private final CacheKeyGenerator cacheKeyGenerator;

   public MethodMetaData(Method method,
                         AggregatedParameterMetaData aggregatedParameterMetaData,
                         Set<Annotation> annotations,
                         CacheKeyGenerator cacheKeyGenerator,
                         A cacheAnnotation,
                         String cacheName) {

      this.method = method;
      this.aggregatedParameterMetaData = aggregatedParameterMetaData;
      this.annotations = unmodifiableSet(annotations);
      this.cacheKeyGenerator = cacheKeyGenerator;
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
