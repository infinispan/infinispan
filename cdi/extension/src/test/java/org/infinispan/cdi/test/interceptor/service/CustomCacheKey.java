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
package org.infinispan.cdi.test.interceptor.service;

import javax.cache.annotation.CacheKey;
import java.lang.reflect.Method;

/**
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
public class CustomCacheKey implements CacheKey {

   private static final long serialVersionUID = -2393683631229917970L;

   private final Method method;
   private final Object firstParameter;

   public CustomCacheKey(Method method, Object firstParameter) {
      this.method = method;
      this.firstParameter = firstParameter;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      CustomCacheKey that = (CustomCacheKey) o;

      if (firstParameter != null ? !firstParameter.equals(that.firstParameter) : that.firstParameter != null)
         return false;
      if (method != null ? !method.equals(that.method) : that.method != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = method != null ? method.hashCode() : 0;
      result = 31 * result + (firstParameter != null ? firstParameter.hashCode() : 0);
      return result;
   }
}
