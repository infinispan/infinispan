package org.infinispan.query.remote.impl.logging;

import org.infinispan.commons.CacheException;
import org.jboss.logging.BasicLogger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

/**
 * Log abstraction for the remote query module. For this module, message ids
 * ranging from 28001 to 28500 inclusively have been reserved.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
@MessageLogger(projectCode = "ISPN")
public interface Log extends BasicLogger {

   @Message(value = "Unknown field %s in type %s", id = 28001)
   IllegalArgumentException unknownField(String fieldName, String fullyQualifiedTypeName);

   @Message(value = "Field %s from type %s is not indexed", id = 28002)
   IllegalArgumentException fieldIsNotIndexed(String fieldName, String fullyQualifiedTypeName);

   @Message(value = "An exception has occurred during query execution", id = 28003)
   CacheException errorExecutingQuery(@Cause Throwable cause);

   @Message(value = "Querying is not enabled on cache %s", id = 28004)
   CacheException queryingNotEnabled(String cacheName);

   @Message(value = "Field %s from type %s is not analyzed", id = 28005)
   IllegalArgumentException fieldIsNotAnalyzed(String fieldName, String fullyQualifiedTypeName);

   @Message(value = "An exception has occurred during filter execution", id = 28006)
   CacheException errorFiltering(@Cause Throwable cause);

   @Message(value = "The key must be a String : %s", id = 28007)
   CacheException keyMustBeString(Class<?> c);

   @Message(value = "The value must be a String : %s", id = 28008)
   CacheException valueMustBeString(Class<?> c);

   @Message(value = "The key must be a String ending with \".proto\" : %s", id = 28009)
   CacheException keyMustBeStringEndingWithProto(Object key);

   @Message(value = "Failed to parse proto file.", id = 28010)
   CacheException failedToParseProtoFile(@Cause Throwable cause);

   @Message(value = "Failed to parse proto file : %s", id = 28011)
   CacheException failedToParseProtoFile(String fileName, @Cause Throwable cause);
}
