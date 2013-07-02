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
