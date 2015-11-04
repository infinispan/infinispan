package org.infinispan.query.remote.impl.logging;

import org.infinispan.commons.CacheException;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

/**
 * Log abstraction for the remote query module. For this module, message ids
 * ranging from 18001 to 19000 inclusively have been reserved.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
@MessageLogger(projectCode = "ISPN")
public interface Log extends org.infinispan.util.logging.Log {

   @Message(value = "Unknown field %s in type %s", id = 18001)
   IllegalArgumentException unknownField(String fieldName, String fullyQualifiedTypeName);

   @Message(value = "Field %s from type %s is not indexed", id = 18002)
   IllegalArgumentException fieldIsNotIndexed(String fieldName, String fullyQualifiedTypeName);

   @Message(value = "An exception has occurred during query execution", id = 18003)
   CacheException errorExecutingQuery(@Cause Throwable cause);

   @Message(value = "Querying is not enabled on cache %s", id = 18004)
   CacheException queryingNotEnabled(String cacheName);
}
