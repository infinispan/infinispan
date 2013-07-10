package org.infinispan.jcache.logging;

import org.jboss.logging.Cause;
import org.jboss.logging.LogMessage;
import org.jboss.logging.Message;
import org.jboss.logging.MessageLogger;

import javax.cache.CacheException;
import javax.cache.configuration.Configuration;
import javax.cache.transaction.IsolationLevel;
import javax.cache.transaction.Mode;

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
   NullPointerException parameterMustNotBeNull(String parameterName);

   @Message(value = "Incompatible cache value types specified, expected %s but %s was specified", id = 21011)
   ClassCastException incompatibleType(Class<?> type, Class<?> cfgType);

   @Message(value = "Cache %s was defined with specific types Cache<%s, %s> in which case CacheManager.getCache(String, Class, Class) must be used", id = 21012)
   IllegalArgumentException unsafeTypedCacheRequest(String cacheName, Class<?> keyType, Class<?> valueType);

   @Message(value = "Can't use store-by-reference and transactions together", id = 21013)
   IllegalArgumentException storeByReferenceAndTransactionsNotAllowed();

   @Message(value = "Incompatible IsolationLevel %s and tx mode %s", id = 21014)
   IllegalArgumentException incompatibleIsolationLevelAndTransactionMode(IsolationLevel level, Mode txMode);

   @Message(value = "Cache %s already registered with configuration %s, and can not be registered again with a new given configuration %s", id = 21015)
   CacheException cacheAlreadyRegistered(String cacheName, Configuration cacheCfg, Configuration newCfg);

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
