package org.infinispan.query.remote.impl.logging;

import static org.infinispan.util.logging.Log.LOG_ROOT;
import static org.jboss.logging.Logger.Level.WARN;

import java.lang.invoke.MethodHandles;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.CacheException;
import org.infinispan.protostream.descriptors.JavaType;
import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;
import org.jboss.logging.annotations.ValidIdRange;

/**
 * Log abstraction for the remote query module.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
@MessageLogger(projectCode = "ISPN")
@ValidIdRange(min = 28001, max = 28500)
public interface Log extends BasicLogger {
   Log CONTAINER = Logger.getMessageLogger(MethodHandles.lookup(), Log.class, LOG_ROOT + "CONTAINER");

   /*@Message(value = "Unknown field %s in type %s", id = 28001)
   IllegalArgumentException unknownField(String fieldName, String fullyQualifiedTypeName);

   @Message(value = "Field %s from type %s is not indexed", id = 28002)
   IllegalArgumentException fieldIsNotIndexed(String fieldName, String fullyQualifiedTypeName);

   @Message(value = "An exception has occurred during query execution", id = 28003)
   CacheException errorExecutingQuery(@Cause Throwable cause);*/

   @Message(value = "Querying is not enabled on cache %s", id = 28004)
   CacheException queryingNotEnabled(String cacheName);

   /*@Message(value = "Field %s from type %s is not analyzed", id = 28005)
   IllegalArgumentException fieldIsNotAnalyzed(String fieldName, String fullyQualifiedTypeName);

   @Message(value = "An exception has occurred during filter execution", id = 28006)
   CacheException errorFiltering(@Cause Throwable cause);*/

   @Message(value = "The key must be a String : %s", id = 28007)
   CacheException keyMustBeString(Class<?> c);

   @Message(value = "The value must be a String : %s", id = 28008)
   CacheException valueMustBeString(Class<?> c);

   @Message(value = "The key must be a String ending with \".proto\" : %s", id = 28009)
   CacheException keyMustBeStringEndingWithProto(Object key);

//   @Message(value = "Failed to parse proto file.", id = 28010)
//   CacheException failedToParseProtoFile(@Cause Throwable cause);

   @Message(value = "Failed to parse proto file : %s", id = 28011)
   CacheException failedToParseProtoFile(String fileName, @Cause Throwable cause);

   @Message(value = "Error during execution of protostream serialization context initializer", id = 28013)
   CacheException errorInitializingSerCtx(@Cause Throwable cause);

   @Message(value = "The '%s' cache does not support commands of type %s", id = 28014)
   CacheException cacheDoesNotSupportCommand(String cacheName, String commandType);

   @Message(value = "Cache '%s' with storage type '%s' cannot be queried. Please configure the cache encoding as " +
         "'application/x-protostream' or 'application/x-java-object'", id = 28015)
   CacheException cacheNotQueryable(String cacheName, String storage);

   @LogMessage(level = WARN)
   @Message(id = 28016, value = "Query performed in a cache ('%s') that has an unknown format configuration. " +
         "Please configure the cache encoding as 'application/x-protostream' or 'application/x-java-object'")
   void warnNoMediaType(String cacheName);

   /*@Message(id = 28017, value = "Type '%s' was not declared as an indexed entity. " +
         "Please declare it in the indexing configuration of your cache and ensure the type is defined by a schema before this cache is started.")
   CacheConfigurationException indexingUndeclaredType(String typeName);*/

   @Message(id = 28018, value = "It is not possible to create indexes for a field having type %s. Field: %s.")
   CacheException fieldTypeNotIndexable(String typeName, String fieldName);

   /*@LogMessage(level = INFO)
   @Message(id = 28019, value = "Registering protostream serialization context initializer: %s")
   void registeringSerializationContextInitializer(String className);*/

   /*@Message(id = 28020, value = "It is not possible to create indexes for a field having type %s. Field: %s.")
   CacheException typeNotIndexable(String typeName, String fieldName);*/

   @Message(id = 28021, value = "The configured indexed-entity type '%s' must be indexed. Please annotate it with @Indexed and make sure at least one field has some indexing annotation, or remove it from the configuration.")
   CacheConfigurationException typeNotIndexed(String typeName);

   @Message(id = 28022, value = "The declared indexed type '%s' is not known. Please register its proto schema file first")
   CacheConfigurationException unknownType(String typeName);

   @Message(id = 28023, value = "Dimension attribute is required for the vector field '%s'")
   CacheException dimensionAttributeRequired(String fieldName);

   @Message(id = 28024, value = "The type '%s' is not valid for the vector field '%s'. Valid types are: '%s' or '%s'.")
   CacheException wrongTypeForVector(String typeName, String fieldName, JavaType validType1, JavaType validType2);

   @Message(id = 28025, value = "The key property name '%s' is already used by another mapped field. You can change it setting the attribute 'keyPropertyName'.")
   CacheConfigurationException keyPropertyNameAlreadyInUse(String keyPropertyName);

   @LogMessage(level = WARN)
   @Message(id = 28026, value = "Error indexing protobuf entity.")
   void errorIndexingProtobufEntry(@Cause Throwable cause);

   @LogMessage(level = WARN)
   @Message(id = 28027, value = "Error unmarshalling protobuf entity.")
   void errorUnmarshallingProtobufEntity(@Cause Throwable cause);

}
