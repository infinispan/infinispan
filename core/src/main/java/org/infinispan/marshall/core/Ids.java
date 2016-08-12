package org.infinispan.marshall.core;

/**
 * Indexes for object types. These are currently limited to being unsigned ints, so valid values are considered those
 * in the range of 0 to 254. Please note that the use of 255 is forbidden since this is reserved for foreign, or user
 * defined, externalizers.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
public interface Ids extends org.infinispan.commons.marshall.Ids {
   // No internal externalizer should use this upper limit Id or anything higher than that.
   int MAX_ID = 255;

   int ARRAY_LIST = 0;
   int LINKED_LIST = 1;
   int MAPS = 2;
   int JDK_SETS = 3;
   int SINGLETON_LIST = 4;
   // responses
   int SUCCESSFUL_RESPONSE = 5;
   int EXTENDED_RESPONSE = 6;
   int EXCEPTION_RESPONSE = 7;
   int UNSUCCESSFUL_RESPONSE = 8;
   int REQUEST_IGNORED_RESPONSE = 9;
   // entries and values
   int IMMORTAL_ENTRY = 10;
   int MORTAL_ENTRY = 11;
   int TRANSIENT_ENTRY = 12;
   int TRANSIENT_MORTAL_ENTRY = 13;
   int IMMORTAL_VALUE = 14;
   int MORTAL_VALUE = 15;
   int TRANSIENT_VALUE = 16;
   int TRANSIENT_MORTAL_VALUE = 17;
   // internal collections (id=18 no longer in use, might get reused at a later stage)
   // id=19 moved to parent interface
   int ATOMIC_HASH_MAP = 20;
   int INT_SUMMARY_STATISTICS = 21;
   int LONG_SUMMARY_STATISTICS = 22;
   int DOUBLE_SUMMARY_STATISTICS = 23;
   // others
   int GLOBAL_TRANSACTION = 38;
   int JGROUPS_ADDRESS = 39;
   int MARSHALLED_VALUE = 40;
   // 41 no longer in use, used to be TransactionLog.LogEntry
   // 42 no longer in use
   int DEADLOCK_DETECTING_GLOBAL_TRANSACTION = 43;

   // 44 and 45 no longer in use, used to belong to tree module
   int ATOMIC_HASH_MAP_DELTA = 46;
   int ATOMIC_PUT_OPERATION = 47;
   int ATOMIC_REMOVE_OPERATION = 48;
   int ATOMIC_CLEAR_OPERATION = 49;
   int DEFAULT_CONSISTENT_HASH = 51;
   int REPLICATED_CONSISTENT_HASH = 52;
   int UNSURE_RESPONSE = 54;
   // 55 - 56 no longer in use since server modules can now register their own externalizers
   int BYTE_ARRAY_KEY = 57;
   // 58 - 59 no longer in use since server modules can now register their own externalizers
   int JGROUPS_TOPOLOGY_AWARE_ADDRESS = 60;
   int TOPOLOGY_AWARE_CH = 61;
   // commands (ids between 21 and 37 both inclusive and 50 and 53, are no longer in use, might get reused at a later stage)
   int REPLICABLE_COMMAND = 62;

   // 63 no longer in use. used to be RemoteTransactionLogDetails

   int XID = 66;
   int XID_DEADLOCK_DETECTING_GLOBAL_TRANSACTION = 67;
   int XID_GLOBAL_TRANSACTION = 68;

   int IN_DOUBT_TX_INFO = 70;

   /* 71-73 reserved in org.infinispan.commons.marshall.Ids */

   int CACHE_RPC_COMMAND = 74;

   int CACHE_TOPOLOGY = 75;

   // Metadata entries and values
   int METADATA_IMMORTAL_ENTRY = 76;
   int METADATA_MORTAL_ENTRY = 77;
   int METADATA_TRANSIENT_ENTRY = 78;
   int METADATA_TRANSIENT_MORTAL_ENTRY = 79;
   int METADATA_IMMORTAL_VALUE = 80;
   int METADATA_MORTAL_VALUE = 81;
   int METADATA_TRANSIENT_VALUE = 82;
   int METADATA_TRANSIENT_MORTAL_VALUE = 83;

   int TRANSACTION_INFO = 84;

   int FLAG = 85;

   int STATE_CHUNK = 86;
   int CACHE_JOIN_INFO = 87;

   /* 88-90 reserved in org.infinispan.commons.marshall.Ids */

   int DEFAULT_CONSISTENT_HASH_FACTORY = 91;
   int REPLICATED_CONSISTENT_HASH_FACTORY = 92;
   int SYNC_CONSISTENT_HASH_FACTORY = 93;
   int TOPOLOGY_AWARE_CONSISTENT_HASH_FACTORY = 94;
   int TOPOLOGY_AWARE_SYNC_CONSISTENT_HASH_FACTORY = 95;
   int SIMPLE_CLUSTERED_VERSION = 96;
   int DELTA_COMPOSITE_KEY = 97;

   int EMBEDDED_METADATA = 98;

   int NUMERIC_VERSION = 99;
   int SCOPED_KEY = 100;

   int NON_EXISTING_VERSION = 101;

   int CACHE_NOT_FOUND_RESPONSE = 102;
   int KEY_VALUE_PAIR_ID = 103;
   int INTERNAL_METADATA_ID = 104;
   int MARSHALLED_ENTRY_ID = 105;

   /* 106 -120 reserved in org.infinispan.commons.marshall.Ids */

   int ENUM_SET_ID = 121;
   int LIST_ARRAY = 122;

   int SIMPLE_COLLECTION_KEY_FILTER = 123;
   int KEY_FILTER_AS_KEY_VALUE_FILTER = 124;
   int CLUSTER_EVENT = 125;
   int CLUSTER_LISTENER_REMOVE_CALLABLE = 126;
   int CLUSTER_LISTENER_REPLICATE_CALLABLE = 127;
   int CLUSTER_EVENT_CALLABLE = 128;
   int X_SITE_STATE = 129;
   int COMPOSITE_KEY_VALUE_FILTER = 130;
   int DELTA_MAPREDUCE_LIST_ID = 131;
   int DELTA_AWARE_MAPREDUCE_LIST_ID = 132;
   int CACHE_STATUS_RESPONSE = 133;
   int CACHE_EVENT_CONVERTER_AS_CONVERTER = 134;
   int CACHE_EVENT_FILTER_AS_KEY_VALUE_FILTER = 135;
   int CONVERTER_AS_CACHE_EVENT_CONVERTER = 136;
   int KEY_FILTER_AS_CACHE_EVENT_FILTER = 137;
   int KEY_VALUE_FILTER_AS_CACHE_EVENT_FILTER = 138;
   int NULL_VALUE_CONVERTER = 139;
   int ACCEPT_ALL_KEY_VALUE_FILTER = 140;
   int MANAGER_STATUS_RESPONSE = 141;
   int MULTI_CLUSTER_EVENT_CALLABLE = 142;
   int COMPOSITE_KEY_FILTER = 143;
   int KEY_VALUE_FILTER_AS_KEY_FILTER = 144;
   int CACHE_EVENT_FILTER_CONVERTER_AS_KEY_VALUE_FILTER_CONVERTER = 145;

   int INTERMEDIATE_OPERATIONS = 146;
   int TERMINAL_OPERATIONS = 147;
   int STREAM_MARSHALLING = 148;

   int COMMAND_INVOCATION_ID = 149;
   int CACHE_FILTERS = 150;

   int OPTIONAL = 151;

   // Functional
   int META_PARAMS_INTERNAL_METADATA = 152;
   int META_PARAMS = 153;

   // TODO: Add other meta params
   int META_LIFESPAN = 154;
   int META_ENTRY_VERSION = 155;
   int NUMERIC_ENTRY_VERSION = 156;

   int READ_WRITE_SNAPSHOT_VIEW = 157;

   // 158 - 160 used by functions in commons
   int AVAILABILITY_MODE = 161;
   int VALUE_MATCHER = 162;

   int HASH_FUNCTION_PARTITIONER = 163;
   int SYNC_REPLICATED_CONSISTENT_HASH_FACTORY = 164;

   int AFFINITY_FUNCTION_PARTITIONER = 165;

   int PERSISTENT_UUID = 166;

   int MIME_CACHE_ENTRY = 167;

   int UUID = 168;
}
