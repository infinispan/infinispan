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
package org.jboss.seam.infinispan.util;

import org.infinispan.CacheException;

import javax.cache.interceptor.CacheKey;
import javax.cache.interceptor.CacheKeyGenerator;
import javax.interceptor.InvocationContext;
import java.lang.reflect.Method;

/**
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
public final class CacheHelper {

   /**
    * Disable instantiation.
    */
   private CacheHelper() {
   }

   /**
    * Returns the default cache name associated to the given method according to JSR-107 specification.
    *
    * @param method The method.
    * @return The default cache name for the given method.
    */
   public static String getDefaultMethodCacheName(Method method) {
      Contracts.assertNotNull(method, "method parameter cannot be null");

      int i = 0;
      int nbParameters = method.getParameterTypes().length;

      StringBuilder cacheName = new StringBuilder()
            .append(method.getDeclaringClass().getName())
            .append(".")
            .append(method.getName())
            .append("(");

      for (Class<?> oneParameterType : method.getParameterTypes()) {
         cacheName.append(oneParameterType.getName());
         if (i < (nbParameters - 1)) {
            cacheName.append(",");
         }
         i++;
      }

      return cacheName
            .append(")")
            .toString();
   }

   /**
    * Generates a {@link CacheKey} for the given {@link InvocationContext} by using the given {@link CacheKeyGenerator}
    * class.
    *
    * @param cacheKeyGeneratorClass The cache key generator class.
    * @param context                The invocation context.
    * @return An instance of {@code CacheKey} for the given {@code InvocationContext}.
    */
   public static CacheKey generateCacheKey(Class<? extends CacheKeyGenerator> cacheKeyGeneratorClass, InvocationContext context) {
      Contracts.assertNotNull(cacheKeyGeneratorClass, "cacheKeyGeneratorClass parameter cannot be null");
      Contracts.assertNotNull(context, "context parameter cannot be null");

      try {

         CacheKeyGenerator cacheKeyGenerator = cacheKeyGeneratorClass.newInstance();
         return cacheKeyGenerator.generateCacheKey(context);

      } catch (InstantiationException e) {
         throw new CacheException("Cannot instantiate CacheKeyGenerator named '" + cacheKeyGeneratorClass + "'", e);
      } catch (IllegalAccessException e) {
         throw new CacheException("Cannot instantiate CacheKeyGenerator named '" + cacheKeyGeneratorClass + "'", e);
      }
   }
}
