/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.cdi.interceptor.context;

import org.infinispan.cdi.interceptor.context.metadata.MethodMetaData;
import org.infinispan.cdi.interceptor.context.metadata.ParameterMetaData;

import javax.cache.interceptor.CacheInvocationParameter;
import javax.cache.interceptor.CacheKeyGenerator;
import javax.cache.interceptor.CacheKeyInvocationContext;
import javax.interceptor.InvocationContext;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Set;

import static java.util.Arrays.copyOf;
import static java.util.Arrays.deepToString;

/**
 * The {@link CacheKeyInvocationContext} implementation.
 *
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
public class CacheKeyInvocationContextImpl<A extends Annotation> implements CacheKeyInvocationContext<A> {

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
   public <T> T unwrap(Class<T> cls) {
      if (cls.isAssignableFrom(this.getClass())) {
         return cls.cast(this);
      }
      throw new IllegalArgumentException("The provider implementation cannot be unwrapped to " + cls);
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
