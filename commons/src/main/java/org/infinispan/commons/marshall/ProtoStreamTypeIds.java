package org.infinispan.commons.marshall;

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
 * If message/enum types are no longer required, the variable should be commented instead of deleted.
 *
 * @author Ryan Emerson
 * @since 10.0
 */
public interface ProtoStreamTypeIds {


   // 1 byte Ids 0..127 -> Reserved for critical messages used a lot
   int WRAPPED_BYTE_ARRAY = 0;
   int USER_MARSHALLER_BYTES = 1;
   int BYTE_STRING = 2;
   int EMBEDDED_METADATA = 3;
   int EMBEDDED_EXPIRABLE_METADATA = 4;
   int EMBEDDED_LIFESPAN_METADATA = 5;
   int EMBEDDED_MAX_IDLE_METADATA = 6;
   int NUMERIC_VERSION = 7;
   int SIMPLE_CLUSTERED_VERSION = 8;
   int JGROUPS_ADDRESS = 9;

   // Priority counter values
   int COUNTER_VALUE = 125;
   int STRONG_COUNTER_KEY = 126;
   int WEAK_COUNTER_KEY = 127;

   // 2 byte Ids 128..16383
   // Commons range 128 -> 999
   int COMMONS_START = 128;
   int MEDIA_TYPE = COMMONS_START;

   // Core range 1000 -> 3999
   int CORE_START = COMMONS_START + 872;
   int EVENT_LOG_CATEGORY = CORE_START + 1;
   int EVENT_LOG_LEVEL = CORE_START + 2;
   int MARSHALLED_VALUE_IMPL = CORE_START + 3;
   int META_PARAMS_INTERNAL_METADATA = CORE_START + 4;
   int REMOTE_METADATA = CORE_START + 5;

   // Counter range 4000 -> 4199
   int COUNTERS_START = CORE_START + 3000;
   int COUNTER_STATE = COUNTERS_START + 1;

   // Query range 4200 -> 4399
   int QUERY_START = COUNTERS_START + 200;
   int KNOWN_CLASS_KEY = QUERY_START + 1;

   // Remote Query range 4400 -> 4599
   int REMOTE_QUERY_START = QUERY_START + 200;
   int PROTOBUF_VALUE_WRAPPER = REMOTE_QUERY_START + 1;

   // Lucene Directory 4600 -> 4799
   int LUCENE_START = REMOTE_QUERY_START + 200;
   int CHUNK_CACHE_KEY = LUCENE_START + 1;
   int FILE_CACHE_KEY = LUCENE_START + 2;
   int FILE_LIST_CACHE_KEY = LUCENE_START + 3;
   int FILE_METADATA = LUCENE_START + 4;
   int FILE_READ_LOCK_KEY = LUCENE_START + 5;
   int FILE_LIST_CACHE_VALUE = LUCENE_START + 6;

   // Scripting 4800 -> 4999
   int SCRIPTING_START = LUCENE_START + 200;
   int EXECUTION_MODE = SCRIPTING_START + 1;
   int SCRIPT_METADATA = SCRIPTING_START + 2;

   // Memcached 5000 -> 5099
   int MEMCACHED_START = SCRIPTING_START + 200;
   int MEMCACHED_METADATA = MEMCACHED_START + 1;

   // RocksDB 5100 -> 5199
   int ROCKSDB_START = MEMCACHED_START + 100;
   int ROCKSDB_EXPIRY_BUCKET = ROCKSDB_START + 1;

   // Event-logger 5200 -> 5299
   int EVENT_LOGGER_START = ROCKSDB_START + 100;
   int SERVER_EVENT_IMPL = EVENT_LOGGER_START + 1;
}
