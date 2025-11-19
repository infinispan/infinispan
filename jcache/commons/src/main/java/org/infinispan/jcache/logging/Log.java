package org.infinispan.jcache.logging;


import static org.jboss.logging.Logger.Level.ERROR;
import static org.jboss.logging.Logger.Level.WARN;

import java.io.Closeable;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.util.Collection;

import javax.cache.CacheException;
import javax.cache.configuration.Configuration;
import javax.cache.processor.EntryProcessorException;

import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;
import org.jboss.logging.annotations.ValidIdRange;

/**
 * Logger for JCache implementation.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
@MessageLogger(projectCode = "ISPN")
@ValidIdRange(min = 19001, max = 20000)
public interface Log extends BasicLogger {
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

   static Log getLog(Class<?> clazz) {
      return Logger.getMessageLogger(MethodHandles.lookup(), Log.class, clazz.getName());
   }

   @Message(value = "Allocation stack trace:", id = 19001)
   LeakDescription cacheManagerNotClosed();

   @LogMessage(level = WARN)
   @Message(value = "Closing leaked cache manager", id = 19002)
   void leakedCacheManager(@Cause Throwable allocationStackTrace);

//   @Message(value = "Method named '%s' is not annotated with CacheResult, CachePut, CacheRemoveEntry or CacheRemoveAll", id = 19003)
//   IllegalArgumentException methodWithoutCacheAnnotation(String methodName);
//
//   @Message(value = "Method named '%s' must have at least one parameter annotated with @CacheValue", id = 19004)
//   CacheException cachePutMethodWithoutCacheValueParameter(String methodName);
//
//   @Message(value = "Method named '%s' must have only one parameter annotated with @CacheValue", id = 19005)
//   CacheException cachePutMethodWithMoreThanOneCacheValueParameter(String methodName);
//
//   @Message(value = "Method named '%s' is annotated with CacheRemoveEntry but doesn't specify a cache name", id = 19006)
//   CacheException cacheRemoveEntryMethodWithoutCacheName(String methodName);
//
//   @Message(value = "Method named '%s' is annotated with CacheRemoveAll but doesn't specify a cache name", id = 19007)
//   CacheException cacheRemoveAllMethodWithoutCacheName(String methodName);
//
//   @Message(value = "Unable to instantiate CacheKeyGenerator with type '%s'", id = 19008)
//   CacheException unableToInstantiateCacheKeyGenerator(Class<?> type, @Cause Throwable cause);

//   @Message(value = "The provider implementation cannot be unwrapped to '%s'", id = 19009)
//   IllegalArgumentException unableToUnwrapProviderImplementation(Class<?> type);

   @Message(value = "'%s' parameter must not be null", id = 19010)
   NullPointerException parameterMustNotBeNull(String parameterName);

   @Message(value = "Incompatible cache value types specified, expected %s but %s was specified", id = 19011)
   ClassCastException incompatibleType(Class<?> type, Class<?> cfgType);

//   @Message(value = "Cache %s was defined with specific types Cache<%s, %s> in which case CacheManager.getCache(String, Class, Class) must be used", id = 19012)
//   IllegalArgumentException unsafeTypedCacheRequest(String cacheName, Class<?> keyType, Class<?> valueType);
//
//   @Message(value = "Can't use store-by-reference and transactions together", id = 19013)
//   IllegalArgumentException storeByReferenceAndTransactionsNotAllowed();

   @Message(value = "Cache %s already registered with configuration %s, and can not be registered again with a new given configuration %s", id = 19015)
   CacheException cacheAlreadyRegistered(String cacheName, Configuration cacheCfg, Configuration newCfg);

   @Message(value = "Unknown expiry operation: %s", id = 19016)
   IllegalStateException unknownExpiryOperation(String op);

   @LogMessage(level = ERROR)
   @Message(value = "Error loading %s keys from persistence store", id = 19017)
   void errorLoadingAll(Collection<?> keysToLoad, @Cause Throwable t);

   @Message(value = "The configuration class %s is not supported by this implementation", id = 19018)
   IllegalArgumentException configurationClassNotSupported(Class clazz);

   @Message(value = "Entry processing failed", id = 19019)
   EntryProcessorException entryProcessingFailed(@Cause Throwable t);

   @Message(value = "Cache manager %s has status %s", id = 19020)
   IllegalStateException cacheManagerClosed(URI managerURI, Object status);

   @Message(value = "Cache %s on %s has status %s", id = 19021)
   IllegalStateException cacheClosed(String cacheName, URI managerURI, Object status);

   @Message(value = "Cache named '%s' was not found.", id = 19022)
   CacheException cacheNotFound(String cacheName);

   @Message(value = "Cache is closed.", id = 19023)
   IllegalStateException cacheClosed();

//   @Message(value = "'%s' parameter must not contain null keys", id = 19024)
//   NullPointerException parameterMustNotContainNullKeys(String parameterName);

//   @Message(value = "'%s' parameter must not contain null values", id = 19025)
//   NullPointerException parameterMustNotContainNullValues(String parameterName);

//   @Message(value = "'%s' parameter must not contain null elements", id = 19026)
//   NullPointerException parameterMustNotContainNullElements(String parameterName);

//   @Message(value = "Failed to add local cache '%s' on the server", id = 19027)
//   CacheException cacheCreationFailed(String cacheName, @Cause Throwable t);

//   @Message(value = "The server management operation failed.", id = 19028)
//   CacheException serverManagementOperationFailed(@Cause Throwable t);

   @LogMessage(level = WARN)
   @Message(value = "Timeout waiting event notification for cache operation.", id = 19029)
   void timeoutWaitingEvent();

   @Message(value = "Cache manager is already closed.", id = 19030)
   IllegalStateException cacheManagerClosed();

   @LogMessage(level = WARN)
   @Message(value = "Error closing %s", id = 19031)
   void errorClosingCloseable(Closeable closeable, @Cause Exception e);

   @LogMessage(level = WARN)
   @Message(value = "Exception while getting expiry duration. Fallback to default duration eternal.", id = 19032)
   void getExpiryHasThrown(@Cause Throwable t);

//   @Message(value = "Unable to instantiate CacheResolverFactory with type '%s'", id = 19033)
//   CacheException unableToInstantiateCacheResolverFactory(Class<?> type, @Cause Throwable cause);

}
