package org.infinispan.commons.marshall;

import org.infinispan.protostream.WrappedMessage;

/**
 * TypeIds used by protostream in place of FQN message/enum names to reduce payload size.
 * <p>
 * ONCE SET VALUES IN THIS CLASS MUST NOT BE CHANGED AS IT WILL BREAK BACKWARDS COMPATIBILITY.
 * <p>
 * Values must in the range 0..65535, as this is marked for internal infinispan use by the protostream project.
 * <p>
 * TypeIds are written as a variable length uint32, so Ids in the range 0..127 should be prioritised for frequently
 * marshalled classes.
 * <p>
 * Message names should not end in _LOWER_BOUND as this is used by ProtoStreamTypeIdsUniquenessTest.
 * <p>
 * If message/enum types are no longer required, the variable should be commented instead of deleted.
 *
 * @author Ryan Emerson
 * @since 10.0
 */
public interface ProtoStreamTypeIds {

   // 1 byte Ids 0..127 -> Reserved for critical messages used a lot
   int WRAPPED_MESSAGE = WrappedMessage.PROTOBUF_TYPE_ID; // Id 0 is reserved for ProtoStream WrappedMessage class
   int WRAPPED_BYTE_ARRAY = 1;
   int MARSHALLABLE_USER_OBJECT = 2;
   int BYTE_STRING = 3;
   int EMBEDDED_METADATA = 4;
   int EMBEDDED_EXPIRABLE_METADATA = 5;
   int EMBEDDED_LIFESPAN_METADATA = 6;
   int EMBEDDED_MAX_IDLE_METADATA = 7;
   int NUMERIC_VERSION = 8;
   int SIMPLE_CLUSTERED_VERSION = 9;
   int JGROUPS_ADDRESS = 10;
   int PROTOBUF_VALUE_WRAPPER = 11;
   int MEDIA_TYPE = 12;

   // Priority counter values
   int COUNTER_VALUE = 125;
   int STRONG_COUNTER_KEY = 126;
   int WEAK_COUNTER_KEY = 127;

   // 2 byte Ids 128..16383
   // Commons range 128 -> 999
   int COMMONS_LOWER_BOUND = 128;

   // Core range 1000 -> 3999
   int CORE_LOWER_BOUND = COMMONS_LOWER_BOUND + 872;
   int EVENT_LOG_CATEGORY = CORE_LOWER_BOUND;
   int EVENT_LOG_LEVEL = CORE_LOWER_BOUND + 1;
   int MARSHALLED_VALUE_IMPL = CORE_LOWER_BOUND + 2;
   int META_PARAMS_INTERNAL_METADATA = CORE_LOWER_BOUND + 3;
   int REMOTE_METADATA = CORE_LOWER_BOUND + 4;
   int UUID = CORE_LOWER_BOUND + 5;

   // Counter range 4000 -> 4199
   int COUNTERS_LOWER_BOUND = CORE_LOWER_BOUND + 3000;
   int COUNTER_STATE = COUNTERS_LOWER_BOUND;
   int COUNTER_CONFIGURATION = COUNTER_STATE + 1;
   int COUNTER_TYPE = COUNTER_CONFIGURATION + 1;
   int COUNTER_STORAGE = COUNTER_TYPE + 1;

   // Query range 4200 -> 4399
   int QUERY_LOWER_BOUND = COUNTERS_LOWER_BOUND + 200;
   //int KNOWN_CLASS_KEY = QUERY_LOWER_BOUND;

   // Remote Query range 4400 -> 4599
   int REMOTE_QUERY_LOWER_BOUND = QUERY_LOWER_BOUND + 200;
   int REMOTE_QUERY_REQUEST = REMOTE_QUERY_LOWER_BOUND;
   int REMOTE_QUERY_RESPONSE = REMOTE_QUERY_LOWER_BOUND + 1;
   int ICKLE_FILTER_RESULT = REMOTE_QUERY_LOWER_BOUND + 2;
   int ICKLE_CONTINUOUS_QUERY_RESULT = REMOTE_QUERY_LOWER_BOUND + 3;

   // Lucene Directory 4600 -> 4799
   int LUCENE_LOWER_BOUND = REMOTE_QUERY_LOWER_BOUND + 200;
   int CHUNK_CACHE_KEY = LUCENE_LOWER_BOUND;
   int FILE_CACHE_KEY = LUCENE_LOWER_BOUND + 1;
   int FILE_LIST_CACHE_KEY = LUCENE_LOWER_BOUND + 2;
   int FILE_METADATA = LUCENE_LOWER_BOUND + 3;
   int FILE_READ_LOCK_KEY = LUCENE_LOWER_BOUND + 4;
   int FILE_LIST_CACHE_VALUE = LUCENE_LOWER_BOUND + 5;

   // Tasks + Scripting 4800 -> 4999
   int SCRIPTING_LOWER_BOUND = LUCENE_LOWER_BOUND + 200;
   int EXECUTION_MODE = SCRIPTING_LOWER_BOUND;
   int SCRIPT_METADATA = SCRIPTING_LOWER_BOUND + 1;
   int DISTRIBUTED_SERVER_TASK = SCRIPT_METADATA + 1;
   int DISTRIBUTED_SERVER_TASK_PARAMETER = DISTRIBUTED_SERVER_TASK + 1;

   // Memcached 5000 -> 5099
   int MEMCACHED_LOWER_BOUND = SCRIPTING_LOWER_BOUND + 200;
   int MEMCACHED_METADATA = MEMCACHED_LOWER_BOUND;

   // RocksDB 5100 -> 5199
   int ROCKSDB_LOWER_BOUND = MEMCACHED_LOWER_BOUND + 100;
   int ROCKSDB_EXPIRY_BUCKET = ROCKSDB_LOWER_BOUND;

   // Event-logger 5200 -> 5299
   int EVENT_LOGGER_LOWER_BOUND = ROCKSDB_LOWER_BOUND + 100;
   int SERVER_EVENT_IMPL = EVENT_LOGGER_LOWER_BOUND;

   // MultiMap 5300 -> 5399
   int MULTIMAP_LOWER_BOUND = EVENT_LOGGER_LOWER_BOUND + 100;
   int MULTIMAP_BUCKET = MULTIMAP_LOWER_BOUND;

   // Server Core 5400 -> 5799
   int SERVER_CORE_LOWER_BOUND = MULTIMAP_LOWER_BOUND + 100;
   int IGNORED_CACHES = SERVER_CORE_LOWER_BOUND;
}
