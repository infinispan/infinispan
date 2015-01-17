package org.infinispan.jcache.annotation;

import java.lang.annotation.Annotation;
import java.util.Set;

import javax.cache.annotation.CacheInvocationParameter;

/**
 * The {@link javax.cache.annotation.CacheInvocationParameter} implementation.
 *
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 * @author Galder Zamarreño
 */
public class CacheInvocationParameterImpl implements CacheInvocationParameter {

   private final ParameterMetaData parameterMetaData;
   private final Object parameterValue;

   public CacheInvocationParameterImpl(ParameterMetaData parameterMetaData, Object parameterValue) {
      this.parameterMetaData = parameterMetaData;
      this.parameterValue = parameterValue;
   }

   @Override
   public Class<?> getRawType() {
      return parameterMetaData.getRawType();
   }

   @Override
   public Object getValue() {
      return parameterValue;
   }

   @Override
   public Set<Annotation> getAnnotations() {
      return parameterMetaData.getAnnotations();
   }

   @Override
   public int getParameterPosition() {
      return parameterMetaData.getPosition();
   }

   @Override
   public String toString() {
      return new StringBuilder()
            .append("CacheInvocationParameterImpl{")
            .append("parameterMetaData=").append(parameterMetaData)
            .append(", parameterValue=").append(parameterValue)
            .append('}')
            .toString();
   }
}
