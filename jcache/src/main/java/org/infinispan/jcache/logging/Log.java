/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other
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

package org.infinispan.jcache.logging;

import org.jboss.logging.Cause;
import org.jboss.logging.LogMessage;
import org.jboss.logging.Message;
import org.jboss.logging.MessageLogger;

import javax.cache.CacheException;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

import static org.jboss.logging.Logger.Level.WARN;

/**
 * Logger for JCache implementation.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
@MessageLogger(projectCode = "ISPN")
public interface Log extends org.infinispan.util.logging.Log {

   @Message(value = "Allocation stack trace:", id = 21001)
   LeakDescription cacheManagerNotClosed();

   @LogMessage(level = WARN)
   @Message(value = "Closing leaked cache manager", id = 21002)
   void leakedCacheManager(@Cause Throwable allocationStackTrace);

   @Message(value = "Method named '%s' is not annotated with CacheResult, CachePut, CacheRemoveEntry or CacheRemoveAll", id = 21003)
   IllegalArgumentException methodWithoutCacheAnnotation(String methodName);

   @Message(value = "Method named '%s' must have at least one parameter annotated with @CacheValue", id = 21004)
   CacheException cachePutMethodWithoutCacheValueParameter(String methodName);

   @Message(value = "Method named '%s' must have only one parameter annotated with @CacheValue", id = 21005)
   CacheException cachePutMethodWithMoreThanOneCacheValueParameter(String methodName);

   @Message(value = "Method named '%s' is annotated with CacheRemoveEntry but doesn't specify a cache name", id = 21006)
   CacheException cacheRemoveEntryMethodWithoutCacheName(String methodName);

   @Message(value = "Method named '%s' is annotated with CacheRemoveAll but doesn't specify a cache name", id = 21007)
   CacheException cacheRemoveAllMethodWithoutCacheName(String methodName);

   @Message(value = "Unable to instantiate CacheKeyGenerator with type '%s'", id = 21008)
   CacheException unableToInstantiateCacheKeyGenerator(Class<?> type, @Cause Throwable cause);

   @Message(value = "The provider implementation cannot be unwrapped to '%s'", id = 21009)
   IllegalArgumentException unableToUnwrapProviderImplementation(Class<?> type);

   @Message(value = "%s parameter must not be null", id = 21010)
   IllegalArgumentException parameterMustNotBeNull(String parameterName);

   class LeakDescription extends Throwable {

      public LeakDescription() {
         //
      }

      public LeakDescription(String message) {
         super(message);
      }

      @Override
      public String toString() {
         // skip the class-name
         return getLocalizedMessage();
      }
   }

}
