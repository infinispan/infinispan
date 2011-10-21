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
package org.infinispan.cdi.interceptor;

import javax.cache.annotation.CacheInvocationParameter;
import javax.cache.annotation.CacheKey;
import javax.cache.annotation.CacheKeyGenerator;
import javax.cache.annotation.CacheKeyInvocationContext;
import javax.enterprise.context.ApplicationScoped;
import java.lang.annotation.Annotation;

import static org.infinispan.cdi.util.Contracts.assertNotNull;

/**
 * Default {@link CacheKeyGenerator} implementation. By default all key parameters of the intercepted method compose the
 * {@link CacheKey}.
 *
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
@ApplicationScoped
public class DefaultCacheKeyGenerator implements CacheKeyGenerator {

   @Override
   public CacheKey generateCacheKey(CacheKeyInvocationContext<? extends Annotation> cacheKeyInvocationContext) {
      assertNotNull(cacheKeyInvocationContext, "cacheKeyInvocationContext parameter must not be null");

      final CacheInvocationParameter[] keyParameters = cacheKeyInvocationContext.getKeyParameters();
      final Object[] keyValues = new Object[keyParameters.length];

      for (int i = 0 ; i < keyParameters.length ; i++) {
         keyValues[i] = keyParameters[i].getValue();
      }

      return new DefaultCacheKey(keyValues);
   }
}
