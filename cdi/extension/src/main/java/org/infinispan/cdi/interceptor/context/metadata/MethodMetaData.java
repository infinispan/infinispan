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
package org.infinispan.cdi.interceptor.context.metadata;

import javax.cache.annotation.CacheKeyGenerator;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Set;

import static java.util.Collections.unmodifiableSet;

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
