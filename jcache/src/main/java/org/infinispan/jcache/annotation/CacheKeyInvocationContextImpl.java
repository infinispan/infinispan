package org.infinispan.jcache.annotation;

import org.infinispan.commons.util.ReflectionUtil;
import org.infinispan.jcache.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.cache.annotation.CacheInvocationParameter;
import javax.cache.annotation.CacheKeyGenerator;
import javax.cache.annotation.CacheKeyInvocationContext;
import javax.interceptor.InvocationContext;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Set;

import static java.util.Arrays.copyOf;
import static java.util.Arrays.deepToString;

/**
 * The {@link javax.cache.annotation.CacheKeyInvocationContext} implementation.
 *
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
public class CacheKeyInvocationContextImpl<A extends Annotation> implements CacheKeyInvocationContext<A> {

   private static final Log log = LogFactory.getLog(CacheKeyInvocationContextImpl.class, Log.class);

   private final InvocationContext invocationContext;
   private final MethodMetaData<A> methodMetaData;
   private final CacheInvocationParameter[] allParameters;
   private final CacheInvocationParameter[] keyParameters;
   private final CacheInvocationParameter valueParameter;

   public CacheKeyInvocationContextImpl(InvocationContext invocationContext, MethodMetaData<A> methodMetaData) {
      this.invocationContext = invocationContext;
      this.methodMetaData = methodMetaData;

      // populate the de parameters
      final Object[] parameters = invocationContext.getParameters();
      final List<ParameterMetaData> parametersMetaData = methodMetaData.getParameters();
      this.allParameters = new CacheInvocationParameter[parameters.length];

      for (int i = 0; i < parameters.length; i++) {
         this.allParameters[i] = new CacheInvocationParameterImpl(parametersMetaData.get(i), parameters[i]);
      }

      // populate the key parameters
      final List<ParameterMetaData> keyParametersMetaData = methodMetaData.getKeyParameters();

      int i = 0;
      this.keyParameters = new CacheInvocationParameter[keyParametersMetaData.size()];
      for (ParameterMetaData oneKeyParameterMetaData : keyParametersMetaData) {
         this.keyParameters[i] = allParameters[oneKeyParameterMetaData.getPosition()];
         i++;
      }

      // initialize the value parameter
      final ParameterMetaData valueParameterMetaData = methodMetaData.getValueParameter();
      this.valueParameter = valueParameterMetaData != null ? allParameters[valueParameterMetaData.getPosition()] : null;
   }

   @Override
   public Object getTarget() {
      return invocationContext.getTarget();
   }

   @Override
   public CacheInvocationParameter[] getAllParameters() {
      return copyOf(allParameters, allParameters.length);
   }

   @Override
   public CacheInvocationParameter[] getKeyParameters() {
      return copyOf(keyParameters, keyParameters.length);
   }

   @Override
   public CacheInvocationParameter getValueParameter() {
      return valueParameter;
   }

   @Override
   public <T> T unwrap(Class<T> clazz) {
      return ReflectionUtil.unwrap(this, clazz);
   }

   @Override
   public Method getMethod() {
      return methodMetaData.getMethod();
   }

   @Override
   public Set<Annotation> getAnnotations() {
      return methodMetaData.getAnnotations();
   }

   @Override
   public A getCacheAnnotation() {
      return methodMetaData.getCacheAnnotation();
   }

   @Override
   public String getCacheName() {
      return methodMetaData.getCacheName();
   }

   public CacheKeyGenerator getCacheKeyGenerator() {
      return methodMetaData.getCacheKeyGenerator();
   }

   @Override
   public String toString() {
      return new StringBuilder()
            .append("CacheKeyInvocationContextImpl{")
            .append("invocationContext=").append(invocationContext)
            .append(", methodMetaData=").append(methodMetaData)
            .append(", allParameters=").append(deepToString(allParameters))
            .append(", keyParameters=").append(deepToString(keyParameters))
            .append(", valueParameter=").append(valueParameter)
            .append('}')
            .toString();
   }
}
