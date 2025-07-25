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
 * If message/enum types are no longer required, and the values have been used in a .Final release, the variable should
 * be commented instead of deleted.
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
   int PRIVATE_METADATA = 13;
   int NODE_VERSION = 14;

   int FLAG = 15;
   int MARSHALLABLE_ARRAY = 16;
   int MARSHALLABLE_COLLECTION = 17;
   int MARSHALLABLE_DEQUE = 18;
   int MARSHALLABLE_LIST = 19;
   int MARSHALLABLE_LAMBDA = 20;
   int MARSHALLABLE_MAP = 21;
   int MARSHALLABLE_OBJECT = 22;
   int MARSHALLABLE_SET = 23;
   int MARSHALLABLE_THROWABLE = 24;
   int COMMAND_INVOCATION_ID = 25;
   int INVALIDATE_COMMAND = 26;
   int PUT_KEY_VALUE_COMMAND = 27;
   int REMOVE_COMMAND = 28;
   int REMOVE_EXPIRED_COMMAND = 29;
   int REPLACE_COMMAND = 30;
   int SUCCESSFUL_ARRAY_RESPONSE = 31;
   int SUCCESSFUL_BOOLEAN_RESPONSE = 32;
   int SUCCESSFUL_BYTES_RESPONSE = 33;
   int SUCCESSFUL_COLLECTION_RESPONSE = 34;
   int SUCCESSFUL_OBJECT_RESPONSE = 35;
   int SUCCESSFUL_MAP_RESPONSE = 36;
   int SUCCESSFUL_IMMORTAL_CACHE_VALUE_RESPONSE = 37;
   int SUCCESSFUL_LONG_RESPONSE = 38;
   int SUCCESSFUL_METADATA_MORTAL_CACHE_VALUE_RESPONSE = 39;
   int SUCCESSFUL_METADATA_IMMORTAL_CACHE_VALUE_RESPONSE = 40;
   int SUCCESSFUL_METADATA_TRANSIENT_CACHE_VALUE_RESPONSE = 41;
   int SUCCESSFUL_METADATA_TRANSIENT_MORTAL_CACHE_VALUE_RESPONSE = 42;
   int SUCCESSFUL_MORTAL_CACHE_VALUE_RESPONSE = 43;
   int SUCCESSFUL_TRANSIENT_CACHE_VALUE_RESPONSE = 44;
   int SUCCESSFUL_TRANSIENT_MORTAL_CACHE_VALUE_RESPONSE = 45;
   int PREPARE_RESPONSE = 46;
   int VALUE_MATCHER = 47;
   int UNSUCCESSFUL_RESPONSE = 48;
   int EXCEPTION_RESPONSE = 49;
   int UNSURE_RESPONSE = 50;
   int CACHE_NOT_FOUND_RESPONSE = 51;
   int REQUEST_UUID = 52;

   // Priority counter values
   int COUNTER_VALUE = 125;
   int STRONG_COUNTER_KEY = 126;
   int WEAK_COUNTER_KEY = 127;

   // 2 byte Ids 128..16383
   // Commons range 128 -> 999
   int COMMONS_LOWER_BOUND = 128;
   int NULL_VALUE = COMMONS_LOWER_BOUND;
   int XID_IMPL = COMMONS_LOWER_BOUND + 1;
   int INTSET_CONCURRENT_SMALL = COMMONS_LOWER_BOUND + 2;
   int INTSET_EMPTY = COMMONS_LOWER_BOUND + 3;
   int INTSET_RANGE = COMMONS_LOWER_BOUND + 4;
   int INTSET_SINGLETON = COMMONS_LOWER_BOUND + 5;
   int INTSET_SMALL = COMMONS_LOWER_BOUND + 6;

   // Core range 1000 -> 3999
   int CORE_LOWER_BOUND = 1000;
   int EVENT_LOG_CATEGORY = CORE_LOWER_BOUND;
   int EVENT_LOG_LEVEL = CORE_LOWER_BOUND + 1;
   int MARSHALLED_VALUE_IMPL = CORE_LOWER_BOUND + 2;
   int META_PARAMS_INTERNAL_METADATA = CORE_LOWER_BOUND + 3;
   int REMOTE_METADATA = CORE_LOWER_BOUND + 4;
   int UUID = CORE_LOWER_BOUND + 5;
   int IRAC_VERSION = CORE_LOWER_BOUND + 6;
   int IRAC_SITE_VERSION = CORE_LOWER_BOUND + 7;
   int IRAC_VERSION_ENTRY = CORE_LOWER_BOUND + 8;
   int IRAC_METADATA = CORE_LOWER_BOUND + 9;
   int ROLE_SET = CORE_LOWER_BOUND + 10;
   int ROLE = CORE_LOWER_BOUND + 11;
   int AUTHORIZATION_PERMISSION = CORE_LOWER_BOUND + 12;
//   int BITSET = CORE_LOWER_BOUND + 13;
   int ADAPTER_CLASS = CORE_LOWER_BOUND + 14;
   int ADAPTER_SET = CORE_LOWER_BOUND + 15;
   int ADAPTER_DOUBLE_SUMMARY_STATISTICS = CORE_LOWER_BOUND + 16;
   int ADAPTER_INT_SUMMARY_STATISTICS = CORE_LOWER_BOUND + 17;
   int ADAPTER_LONG_SUMMARY_STATISTICS = CORE_LOWER_BOUND + 18;
   int ADAPTER_LIST = CORE_LOWER_BOUND + 19;
   int ADAPTER_OPTIONAL = CORE_LOWER_BOUND + 20;
   int ACCEPT_ALL_KEY_VALUE_FILTER = CORE_LOWER_BOUND + 21;
   int AVAILABILITY_MODE = CORE_LOWER_BOUND + 22;
   int BACKUP_MULTI_KEY_ACK_COMMAND = CORE_LOWER_BOUND + 23;
   int BACKUP_NOOP_COMMAND = CORE_LOWER_BOUND + 24;
   int BI_FUNCTION_MAPPER = CORE_LOWER_BOUND + 25;
   int CACHE_AVAILABILITY_UPDATE_COMMAND = CORE_LOWER_BOUND + 26;
   int CACHE_COLLECTORS_COLLECTOR_SUPPLIER = CORE_LOWER_BOUND + 27;
   int CACHE_CONTAINER_ADMIN_FLAG = CORE_LOWER_BOUND + 28;
   int CACHE_ENTRY_GROUP_PREDICATE = CORE_LOWER_BOUND + 29;
   int CACHE_EVENT_CONVERTER_AS_CONVERTER = CORE_LOWER_BOUND + 30;
   int CACHE_EVENT_FILTER_AS_KEY_VALUE_FILTER = CORE_LOWER_BOUND + 31;
   int CACHE_FILTERS_CONVERTER_AS_CACHE_ENTRY_FUNCTION = CORE_LOWER_BOUND + 32;
   int CACHE_FILTERS_FILTER_CONVERTER_AS_CACHE_ENTRY_FUNCTION = CORE_LOWER_BOUND + 33;
   int CACHE_FILTERS_FILTER_CONVERTER_AS_KEY_FUNCTION = CORE_LOWER_BOUND + 34;
   int CACHE_FILTERS_FILTER_CONVERTER_AS_VALUE_FUNCTION = CORE_LOWER_BOUND + 35;
   int CACHE_FILTERS_KEY_VALUE_FILTER_AS_PREDICATE = CORE_LOWER_BOUND + 36;
   int CACHE_FILTERS_NOT_NULL_CACHE_ENTRY_PREDICATE = CORE_LOWER_BOUND + 37;
   int CACHE_INTERMEDIATE_PUBLISHER = CORE_LOWER_BOUND + 38;
   int CACHE_JOIN_INFO = CORE_LOWER_BOUND + 39;
   int CACHE_JOIN_COMMAND = CORE_LOWER_BOUND + 40;
   int CACHE_LEAVE_COMMAND = CORE_LOWER_BOUND + 41;
   int CACHE_MODE = CORE_LOWER_BOUND + 42;
   int CACHE_SHUTDOWN_COMMAND = CORE_LOWER_BOUND + 43;
   int CACHE_SHUTDOWN_REQUEST_COMMAND = CORE_LOWER_BOUND + 44;
   int CACHE_STATE = CORE_LOWER_BOUND + 45;
   int CACHE_STATUS_REQUEST_COMMAND = CORE_LOWER_BOUND + 46;
   int CACHE_STATUS_RESPONSE = CORE_LOWER_BOUND + 47;
   int CACHE_STREAM_INTERMEDIATE_REDUCER = CORE_LOWER_BOUND + 48;
   int CACHE_TOPOLOGY = CORE_LOWER_BOUND + 49;
   int CACHE_TOPOLOGY_PHASE = CORE_LOWER_BOUND + 50;
   int CANCEL_PUBLISHER_COMMAND = CORE_LOWER_BOUND + 51;
   int CHECK_TRANSACTION_RPC_COMMAND = CORE_LOWER_BOUND + 52;
   int CLEAR_COMMAND = CORE_LOWER_BOUND + 53;
   int CLUSTER_EVENT = CORE_LOWER_BOUND + 54;
   int CLUSTER_EVENT_TYPE = CORE_LOWER_BOUND + 55;
   int CLUSTER_LISTENER_REMOVE_CALLABLE = CORE_LOWER_BOUND + 56;
   int CLUSTER_LISTENER_REPLICATE_CALLABLE = CORE_LOWER_BOUND + 57;
   int CLUSTERED_GET_ALL_COMMAND = CORE_LOWER_BOUND + 58;
   int CLUSTERED_GET_COMMAND = CORE_LOWER_BOUND + 59;
   int CONFLICT_RESOLUTION_START_COMMAND = CORE_LOWER_BOUND + 60;
   int COMMIT_COMMAND = CORE_LOWER_BOUND + 61;
   int COMPLETE_TRANSACTION_COMMAND = CORE_LOWER_BOUND + 62;
   int COMPOSITE_KEY_VALUE_FILTER = CORE_LOWER_BOUND + 63;
   int COMPUTE_COMMAND = CORE_LOWER_BOUND + 64;
   int COMPUTE_IF_ABSENT_COMMAND = CORE_LOWER_BOUND + 65;
   int DATA_CONVERSION = CORE_LOWER_BOUND + 66;
   int DEFAULT_CONSISTENT_HASH = CORE_LOWER_BOUND + 67;
   int DEFAULT_CONSISTENT_HASH_FACTORY = CORE_LOWER_BOUND + 68;
   int DELIVERY_GUARANTEE = CORE_LOWER_BOUND + 69;
   int DISTRIBUTED_CACHE_STATS_CALLABLE = CORE_LOWER_BOUND + 70;
   int ENCODER_KEY_MAPPER = CORE_LOWER_BOUND + 71;
   int ENCODER_ENTRY_MAPPER = CORE_LOWER_BOUND + 72;
   int ENCODER_VALUE_MAPPER = CORE_LOWER_BOUND + 73;
   int ENTRY_VIEWS_NO_VALUE_READ_ONLY = CORE_LOWER_BOUND + 74;
   int ENTRY_VIEWS_READ_ONLY_SNAPSHOT = CORE_LOWER_BOUND + 75;
   int ENTRY_VIEWS_READ_WRITE_SNAPSHOT = CORE_LOWER_BOUND + 76;
   int EXCEPTION_ACK_COMMAND = CORE_LOWER_BOUND + 77;
   int EXCEPTION_WRITE_SKEW = CORE_LOWER_BOUND + 78;
   int FUNCTION_MAPPER = CORE_LOWER_BOUND + 79;
   int FUNCTIONAL_PARAMS = CORE_LOWER_BOUND + 80;
   int FUNCTIONAL_STATS_ENVELOPE = CORE_LOWER_BOUND + 81;
   int GET_IN_DOUBT_TRANSACTIONS_COMMAND = CORE_LOWER_BOUND + 82;
   int GET_IN_DOUBT_TX_INFO_COMMAND = CORE_LOWER_BOUND + 83;
   int GLOBAL_TRANSACTION = CORE_LOWER_BOUND + 84;
   int HEART_BEAT_COMMAND = CORE_LOWER_BOUND + 85;
   int IMMORTAL_CACHE_ENTRY = CORE_LOWER_BOUND + 86;
   int IMMORTAL_CACHE_VALUE = CORE_LOWER_BOUND + 87;
   int IN_DOUBT_TX_INFO = CORE_LOWER_BOUND + 88;
   int INITIAL_PUBLISHER_COMMAND = CORE_LOWER_BOUND + 89;
   int INTERNAL_METADATA_IMPL = CORE_LOWER_BOUND + 90;
   int INVALIDATE_L1_COMMAND = CORE_LOWER_BOUND + 91;
   int IRAC_CLEANUP_KEYS_COMMAND = CORE_LOWER_BOUND + 92;
   int IRAC_CLEAR_KEYS_COMMAND = CORE_LOWER_BOUND + 93;
   int IRAC_MANAGER_KEY_INFO = CORE_LOWER_BOUND + 94;
   int IRAC_METADATA_REQUEST_COMMAND = CORE_LOWER_BOUND + 95;
   int IRAC_PRIMARY_PENDING_KEY_CHECK = CORE_LOWER_BOUND + 96;
   int IRAC_PUT_KEY_VALUE_COMMAND = CORE_LOWER_BOUND + 97;
   int IRAC_PUT_MANY_REQUEST = CORE_LOWER_BOUND + 98;
   int IRAC_PUT_MANY_REQUEST_EXPIRE = CORE_LOWER_BOUND + 99;
   int IRAC_PUT_MANY_REQUEST_REMOVE = CORE_LOWER_BOUND + 100;
   int IRAC_PUT_MANY_REQUEST_WRITE = CORE_LOWER_BOUND + 101;
   int IRAC_REQUEST_STATE_COMMAND = CORE_LOWER_BOUND + 102;
   int IRAC_STATE_RESPONSE_COMMAND = CORE_LOWER_BOUND + 103;
   int IRAC_STATE_RESPONSE_COMMAND_STATE = CORE_LOWER_BOUND + 104;
   int IRAC_TOMBSTONE_CHECKOUT_REQUEST = CORE_LOWER_BOUND + 105;
   int IRAC_TOMBSTONE_CLEANUP_COMMAND = CORE_LOWER_BOUND + 106;
   int IRAC_TOMBSTONE_INFO = CORE_LOWER_BOUND + 107;
   int IRAC_TOMBSTONE_PRIMARY_CHECK_COMMAND = CORE_LOWER_BOUND + 108;
   int IRAC_TOMBSTONE_REMOTE_SITE_CHECK_COMMAND = CORE_LOWER_BOUND + 109;
   int IRAC_TOMBSTONE_STATE_RESPONSE_COMMAND = CORE_LOWER_BOUND + 110;
   int IRAC_TOUCH_KEY_REQUEST = CORE_LOWER_BOUND + 111;
   int IRAC_UPDATE_VERSION_COMMAND = CORE_LOWER_BOUND + 112;
   int KEY_PUBLISHER_RESPONSE = CORE_LOWER_BOUND + 113;
   int KEY_VALUE_FILTER_AS_CACHE_EVENT_FILTER = CORE_LOWER_BOUND + 114;
   int KEY_VALUE_FILTER_CONVERTER_AS_CACHE_EVENT_FILTER_CONVERTER = CORE_LOWER_BOUND + 115;
   int KEY_VALUE_FILTER_CONVERTER_AS_CACHE_KEY_VALUE_CONVERTER = CORE_LOWER_BOUND + 116;
   int KEY_VALUE_PAIR = CORE_LOWER_BOUND + 117;
   int LOCK_CONTROL_COMMAND = CORE_LOWER_BOUND + 118;
   int MANAGER_STATUS_RESPONSE = CORE_LOWER_BOUND + 119;
   int MERGE_FUNCTION = CORE_LOWER_BOUND + 120;
   int METADATA_IMMORTAL_ENTRY = CORE_LOWER_BOUND + 121;
   int METADATA_IMMORTAL_VALUE = CORE_LOWER_BOUND + 122;
   int METADATA_MORTAL_ENTRY = CORE_LOWER_BOUND + 123;
   int METADATA_MORTAL_VALUE = CORE_LOWER_BOUND + 124;
   int METADATA_TRANSIENT_CACHE_ENTRY = CORE_LOWER_BOUND + 125;
   int METADATA_TRANSIENT_CACHE_VALUE = CORE_LOWER_BOUND + 126;
   int METADATA_TRANSIENT_MORTAL_CACHE_ENTRY = CORE_LOWER_BOUND + 127;
   int METADATA_TRANSIENT_MORTAL_CACHE_VALUE = CORE_LOWER_BOUND + 128;
   int META_PARAMS_ENTRY_VERSION = CORE_LOWER_BOUND + 129;
   int META_PARAMS_LIFESPAN = CORE_LOWER_BOUND + 130;
   int META_PARAMS_MAX_IDLE = CORE_LOWER_BOUND + 131;
   int MORTAL_CACHE_ENTRY = CORE_LOWER_BOUND + 132;
   int MORTAL_CACHE_VALUE = CORE_LOWER_BOUND + 133;
   int MULTI_CLUSTER_EVENT_COMMAND = CORE_LOWER_BOUND + 134;
   int MULTI_CLUSTER_EVENT_COMMAND_UUID_MAP = CORE_LOWER_BOUND + 135;
   int MULTI_ENTRIES_FUNCTIONAL_BACKUP_WRITE_COMMAND = CORE_LOWER_BOUND + 136;
   int MULTI_KEY_FUNCTIONAL_BACKUP_WRITE_COMMAND = CORE_LOWER_BOUND + 137;
   int MUTATIONS_READ_WRITE = CORE_LOWER_BOUND + 138;
   int MUTATIONS_READ_WRITE_WITH_VALUE = CORE_LOWER_BOUND + 139;
   int MUTATIONS_WRITE = CORE_LOWER_BOUND + 140;
   int MUTATIONS_WRITE_WITH_VALUE = CORE_LOWER_BOUND + 141;
   int NEXT_PUBLISHER_COMMAND = CORE_LOWER_BOUND + 142;
   int PREPARE_COMMAND = CORE_LOWER_BOUND + 143;
   int PUBLISHER_RESPONSE = CORE_LOWER_BOUND + 144;
   int PUBLISHER_HANDLER_SEGMENT_RESPONSE = CORE_LOWER_BOUND + 145;
   int PUBLISHER_TRANSFORMERS_IDENTITY_TRANSFORMER = CORE_LOWER_BOUND + 146;
   int PUT_MAP_BACKUP_WRITE_COMMAND = CORE_LOWER_BOUND + 147;
   int PUT_MAP_COMMAND = CORE_LOWER_BOUND + 148;
   int READ_ONLY_KEY_COMMAND = CORE_LOWER_BOUND + 149;
   int READ_ONLY_MANY_COMMAND = CORE_LOWER_BOUND + 150;
   int READ_WRITE_KEY_COMMAND = CORE_LOWER_BOUND + 151;
   int READ_WRITE_KEY_VALUE_COMMAND = CORE_LOWER_BOUND + 152;
   int READ_WRITE_MANY_COMMAND = CORE_LOWER_BOUND + 153;
   int READ_WRITE_MANY_ENTRIES_COMMAND = CORE_LOWER_BOUND + 154;
   int REBALANCE_PHASE_CONFIRM_COMMAND = CORE_LOWER_BOUND + 155;
   int REBALANCE_POLICY_UPDATE_COMMAND = CORE_LOWER_BOUND + 156;
   int REBALANCE_STATUS = CORE_LOWER_BOUND + 157;
   int REBALANCE_START_COMMAND = CORE_LOWER_BOUND + 158;
   int REBALANCE_STATUS_REQUEST_COMMAND = CORE_LOWER_BOUND + 159;
   int REDUCTION_PUBLISHER_REQUEST_COMMAND = CORE_LOWER_BOUND + 160;
   int REPLICABLE_MANAGER_FUNCTION_COMMAND = CORE_LOWER_BOUND + 161;
   int REPLICATED_CONSISTENT_HASH = CORE_LOWER_BOUND + 162;
   int REPLICATED_CONSISTENT_HASH_FACTORY = CORE_LOWER_BOUND + 163;
   int REPLICABLE_RUNNABLE_COMMAND = CORE_LOWER_BOUND + 164;
   int ROLLBACK_COMMAND = CORE_LOWER_BOUND + 165;
   int SCOPE_FILTER = CORE_LOWER_BOUND + 166;
   int SCOPED_STATE = CORE_LOWER_BOUND + 167;
   int SEGMENT_PUBLISHER_RESULT = CORE_LOWER_BOUND + 168;
   int SINGLE_KEY_BACKUP_WRITE_COMMAND = CORE_LOWER_BOUND + 169;
   int SINGLE_KEY_BACKUP_WRITE_COMMAND_OPERATION = CORE_LOWER_BOUND + 170;
   int SINGLE_KEY_FUNCTIONAL_BACKUP_WRITE_COMMAND = CORE_LOWER_BOUND + 171;
   int SINGLE_KEY_FUNCTIONAL_BACKUP_WRITE_COMMAND_OPERATION = CORE_LOWER_BOUND + 172;
   int SIZE_COMMAND = CORE_LOWER_BOUND + 173;
   int STATE_CHUNK = CORE_LOWER_BOUND + 174;
   int STATE_RESPONSE_COMMAND = CORE_LOWER_BOUND + 175;
   int STATE_TRANSFER_CANCEL_COMMAND = CORE_LOWER_BOUND + 176;
   int STATE_TRANSFER_GET_TRANSACTIONS_COMMAND = CORE_LOWER_BOUND + 177;
   int STATE_TRANSFER_GET_LISTENERS_COMMAND = CORE_LOWER_BOUND + 178;
   int STATE_TRANSFER_START_COMMAND = CORE_LOWER_BOUND + 179;
   int STREAM_LOCKED_STREAM_CACHE_ENTRY_CONSUMER = CORE_LOWER_BOUND + 180;
   int STREAM_LOCKED_STREAM_CACHE_ENTRY_FUNCTION = CORE_LOWER_BOUND + 181;
   int STREAM_BI_CONSUMER_CACHE_OBJECT = CORE_LOWER_BOUND + 182;
   int STREAM_BI_CONSUMER_CACHE_DOUBLE = CORE_LOWER_BOUND + 183;
   int STREAM_BI_CONSUMER_CACHE_INT = CORE_LOWER_BOUND + 184;
   int STREAM_BI_CONSUMER_CACHE_LONG = CORE_LOWER_BOUND + 185;
   int STREAM_INTOP_DISTINCT_OPERATION = CORE_LOWER_BOUND + 186;
   int STREAM_INTOP_FILTER_OPERATION = CORE_LOWER_BOUND + 187;
   int STREAM_INTOP_FLATMAP_OPERATION = CORE_LOWER_BOUND + 188;
   int STREAM_INTOP_FLATMAP_TO_DOUBLE_OPERATION = CORE_LOWER_BOUND + 189;
   int STREAM_INTOP_FLATMAP_TO_INT_OPERATION = CORE_LOWER_BOUND + 190;
   int STREAM_INTOP_FLATMAP_TO_LONG_OPERATION = CORE_LOWER_BOUND + 191;
   int STREAM_INTOP_LIMIT_OPERATION = CORE_LOWER_BOUND + 192;
   int STREAM_INTOP_MAP_OPERATION = CORE_LOWER_BOUND + 193;
   int STREAM_INTOP_MAP_TO_DOUBLE_OPERATION = CORE_LOWER_BOUND + 194;
   int STREAM_INTOP_MAP_TO_INT_OPERATION = CORE_LOWER_BOUND + 195;
   int STREAM_INTOP_MAP_TO_LONG_OPERATION = CORE_LOWER_BOUND + 196;
   int STREAM_INTOP_PEEK_OPERATION = CORE_LOWER_BOUND + 197;
   int STREAM_INTOP_PRIMITIVE_DOUBLE_BOXED_OPERATION = CORE_LOWER_BOUND + 198;
   int STREAM_INTOP_PRIMITIVE_DOUBLE_DISTINCT_OPERATION = CORE_LOWER_BOUND + 199;
   int STREAM_INTOP_PRIMITIVE_DOUBLE_FILTER_OPERATION = CORE_LOWER_BOUND + 200;
   int STREAM_INTOP_PRIMITIVE_DOUBLE_FLAT_MAP_OPERATION = CORE_LOWER_BOUND + 201;
   int STREAM_INTOP_PRIMITIVE_DOUBLE_LIMIT_OPERATION = CORE_LOWER_BOUND + 202;
   int STREAM_INTOP_PRIMITIVE_DOUBLE_MAP_OPERATION = CORE_LOWER_BOUND + 203;
   int STREAM_INTOP_PRIMITIVE_DOUBLE_MAP_TO_INT_OPERATION = CORE_LOWER_BOUND + 204;
   int STREAM_INTOP_PRIMITIVE_DOUBLE_MAP_TO_LONG_OPERATION = CORE_LOWER_BOUND + 205;
   int STREAM_INTOP_PRIMITIVE_DOUBLE_MAP_TO_OBJ_OPERATION = CORE_LOWER_BOUND + 206;
   int STREAM_INTOP_PRIMITIVE_DOUBLE_PEEK_OPERATION = CORE_LOWER_BOUND + 207;
   int STREAM_INTOP_PRIMITIVE_INT_AS_DOUBLE_OPERATION = CORE_LOWER_BOUND + 208;
   int STREAM_INTOP_PRIMITIVE_INT_AS_LONG_OPERATION = CORE_LOWER_BOUND + 209;
   int STREAM_INTOP_PRIMITIVE_INT_BOXED_OPERATION = CORE_LOWER_BOUND + 210;
   int STREAM_INTOP_PRIMITIVE_INT_DISTINCT_OPERATION = CORE_LOWER_BOUND + 211;
   int STREAM_INTOP_PRIMITIVE_INT_FILTER_OPERATION = CORE_LOWER_BOUND + 212;
   int STREAM_INTOP_PRIMITIVE_INT_FLAT_MAP_OPERATION = CORE_LOWER_BOUND + 213;
   int STREAM_INTOP_PRIMITIVE_INT_LIMIT_OPERATION = CORE_LOWER_BOUND + 214;
   int STREAM_INTOP_PRIMITIVE_INT_MAP_OPERATION = CORE_LOWER_BOUND + 215;
   int STREAM_INTOP_PRIMITIVE_INT_MAP_TO_DOUBLE_OPERATION = CORE_LOWER_BOUND + 216;
   int STREAM_INTOP_PRIMITIVE_INT_MAP_TO_LONG_OPERATION = CORE_LOWER_BOUND + 217;
   int STREAM_INTOP_PRIMITIVE_INT_MAP_TO_OBJ_OPERATION = CORE_LOWER_BOUND + 218;
   int STREAM_INTOP_PRIMITIVE_INT_PEEK_OPERATION = CORE_LOWER_BOUND + 219;
   int STREAM_INTOP_PRIMITIVE_LONG_AS_DOUBLE_OPERATION = CORE_LOWER_BOUND + 220;
   int STREAM_INTOP_PRIMITIVE_LONG_BOXED_OPERATION = CORE_LOWER_BOUND + 221;
   int STREAM_INTOP_PRIMITIVE_LONG_DISTINCT_OPERATION = CORE_LOWER_BOUND + 222;
   int STREAM_INTOP_PRIMITIVE_LONG_FILTER_OPERATION = CORE_LOWER_BOUND + 223;
   int STREAM_INTOP_PRIMITIVE_LONG_FLAT_MAP_OPERATION = CORE_LOWER_BOUND + 224;
   int STREAM_INTOP_PRIMITIVE_LONG_LIMIT_OPERATION = CORE_LOWER_BOUND + 225;
   int STREAM_INTOP_PRIMITIVE_LONG_MAP_OPERATION = CORE_LOWER_BOUND + 226;
   int STREAM_INTOP_PRIMITIVE_LONG_MAP_TO_DOUBLE_OPERATION = CORE_LOWER_BOUND + 227;
   int STREAM_INTOP_PRIMITIVE_LONG_MAP_TO_INT_OPERATION = CORE_LOWER_BOUND + 228;
   int STREAM_INTOP_PRIMITIVE_LONG_MAP_TO_OBJ_OPERATION = CORE_LOWER_BOUND + 229;
   int STREAM_INTOP_PRIMITIVE_LONG_PEEK_OPERATION = CORE_LOWER_BOUND + 230;
   int SYNC_CONSISTENT_HASH = CORE_LOWER_BOUND + 231;
   int SYNC_REPLICATED_CONSISTENT_HASH = CORE_LOWER_BOUND + 232;
   int TOPOLOGY_AWARE_CONSISTENT_HASH = CORE_LOWER_BOUND + 233;
   int TOPOLOGY_AWARE_SYNC_CONSISTENT_HASH = CORE_LOWER_BOUND + 234;
   int TOPOLOGY_UPDATE_COMMAND = CORE_LOWER_BOUND + 235;
   int TOPOLOGY_UPDATE_STABLE_COMMAND = CORE_LOWER_BOUND + 236;
   int TOUCH_COMMAND = CORE_LOWER_BOUND + 237;
   int TRANSACTION_INFO = CORE_LOWER_BOUND + 238;
   int TRANSIENT_CACHE_ENTRY = CORE_LOWER_BOUND + 239;
   int TRANSIENT_CACHE_VALUE = CORE_LOWER_BOUND + 240;
   int TRANSIENT_MORTAL_CACHE_VALUE = CORE_LOWER_BOUND + 241;
   int TX_COMPLETION_NOTIFICATION_COMMAND = CORE_LOWER_BOUND + 242;
   int TX_READ_ONLY_KEY_COMMAND = CORE_LOWER_BOUND + 243;
   int TX_READ_ONLY_MANY_COMMAND = CORE_LOWER_BOUND + 244;
   int VERSIONED_COMMIT_COMMAND = CORE_LOWER_BOUND + 245;
   int VERSIONED_PREPARE_COMMAND = CORE_LOWER_BOUND + 246;
   int VERSIONED_RESULT = CORE_LOWER_BOUND + 247;
   int VERSIONED_RESULTS = CORE_LOWER_BOUND + 248;
   int WRITE_ONLY_KEY_COMMAND = CORE_LOWER_BOUND + 249;
   int WRITE_ONLY_KEY_VALUE_COMMAND = CORE_LOWER_BOUND + 250;
   int WRITE_ONLY_MANY_COMMAND = CORE_LOWER_BOUND + 251;
   int WRITE_ONLY_MANY_ENTRIES_COMMAND = CORE_LOWER_BOUND + 252;
   int XSITE_AMEND_OFFLINE_STATUS_COMMAND = CORE_LOWER_BOUND + 253;
   int XSITE_AUTO_STATE_TRANSFER_RESPONSE = CORE_LOWER_BOUND + 254;
   int XSITE_AUTO_TRANSFER_STATUS_COMMAND = CORE_LOWER_BOUND + 255;
   int XSITE_LOCAL_EVENT_COMMAND = CORE_LOWER_BOUND + 256;
   int XSITE_REMOTE_EVENT_COMMAND = CORE_LOWER_BOUND + 257;
   int XSITE_SINGLE_RPC_COMMAND = CORE_LOWER_BOUND + 258;
   int XSITE_EVENT = CORE_LOWER_BOUND + 259;
   int XSITE_EVENT_TYPE = CORE_LOWER_BOUND + 260;
   int XSITE_SET_STATE_TRANSFER_MODE_COMMAND = CORE_LOWER_BOUND + 261;
   int XSITE_SITE_STATE = CORE_LOWER_BOUND + 262;
   int XSITE_STATE = CORE_LOWER_BOUND + 263;
   int XSITE_STATE_PUSH_COMMAND = CORE_LOWER_BOUND + 264;
   int XSITE_STATE_PUSH_REQUEST = CORE_LOWER_BOUND + 265;
   int XSITE_STATE_TRANSFER_CANCEL_SEND_COMMAND = CORE_LOWER_BOUND + 266;
   int XSITE_STATE_TRANSFER_CLEAR_STATUS_COMMAND = CORE_LOWER_BOUND + 267;
   int XSITE_STATE_TRANSFER_CONTROLLER_REQUEST = CORE_LOWER_BOUND + 268;
   int XSITE_STATE_TRANSFER_FINISH_RECEIVE_COMMAND = CORE_LOWER_BOUND + 269;
   int XSITE_STATE_TRANSFER_FINISH_SEND_COMMAND = CORE_LOWER_BOUND + 270;
   int XSITE_STATE_TRANSFER_MODE = CORE_LOWER_BOUND + 271;
   int XSITE_STATE_TRANSFER_RESTART_SENDING_COMMAND = CORE_LOWER_BOUND + 272;
   int XSITE_STATE_TRANSFER_START_RECEIVE_COMMAND = CORE_LOWER_BOUND + 273;
   int XSITE_STATE_TRANSFER_START_SEND_COMMAND = CORE_LOWER_BOUND + 274;
   int XSITE_STATE_TRANSFER_STATUS = CORE_LOWER_BOUND + 275;
   int XSITE_STATE_TRANSFER_STATUS_REQUEST_COMMAND = CORE_LOWER_BOUND + 276;
   int XSITE_BRING_ONLINE_RESPONSE = CORE_LOWER_BOUND + 277;
   int XSITE_TAKE_OFFLINE_RESPONSE = CORE_LOWER_BOUND + 278;

   // MarshallableFunctions
   int MF_IDENTITY = CORE_LOWER_BOUND + 279;
   int MF_REMOVE = CORE_LOWER_BOUND + 280;
   int MF_REMOVE_IF_VALUE_EQUALS_RETURN_BOOLEAN = CORE_LOWER_BOUND + 281;
   int MF_REMOVE_RETURN_BOOLEAN = CORE_LOWER_BOUND + 282;
   int MF_REMOVE_RETURN_PREV_OR_NULL = CORE_LOWER_BOUND + 283;
   int MF_RETURN_READ_ONLY_FIND_IS_PRESENT = CORE_LOWER_BOUND + 284;
   int MF_RETURN_READ_ONLY_FIND_OR_NULL = CORE_LOWER_BOUND + 285;
   int MF_RETURN_READ_WRITE_FIND = CORE_LOWER_BOUND + 286;
   int MF_RETURN_READ_WRITE_GET = CORE_LOWER_BOUND + 287;
   int MF_RETURN_READ_WRITE_VIEW = CORE_LOWER_BOUND + 288;
   int MF_SET_INTERNAL_CACHE_VALUE = CORE_LOWER_BOUND + 289;
   int MF_SET_VALUE = CORE_LOWER_BOUND + 290;
   int MF_SET_VALUE_META = CORE_LOWER_BOUND + 291;
   int MF_SET_VALUE_IF_ABSENT_RETURN_BOOLEAN = CORE_LOWER_BOUND + 292;
   int MF_SET_VALUE_IF_ABSENT_RETURN_PREV_OR_NULL = CORE_LOWER_BOUND + 293;
   int MF_SET_VALUE_IF_EQUALS_RETURN_BOOLEAN = CORE_LOWER_BOUND + 294;
   int MF_SET_VALUE_IF_PRESENT_RETURN_PREV_OR_NULL = CORE_LOWER_BOUND + 295;
   int MF_SET_VALUE_IF_PRESENT_RETURN_BOOLEAN = CORE_LOWER_BOUND + 296;
   int MF_SET_VALUE_METAS_IF_ABSENT_RETURN_BOOLEAN = CORE_LOWER_BOUND + 297;
   int MF_SET_VALUE_METAS_IF_ABSENT_RETURN_PREV_OR_NULL = CORE_LOWER_BOUND + 298;
   int MF_SET_VALUE_METAS_IF_PRESENT_RETURN_BOOLEAN = CORE_LOWER_BOUND + 299;
   int MF_SET_VALUE_METAS_IF_PRESENT_RETURN_PREV_OR_NULL = CORE_LOWER_BOUND + 300;
   int MF_SET_VALUE_METAS_RETURN_PREV_OR_NULL = CORE_LOWER_BOUND + 301;
   int MF_SET_VALUE_METAS_RETURN_VIEW = CORE_LOWER_BOUND + 302;
   int MF_SET_VALUE_RETURN_PREV_OR_NULL = CORE_LOWER_BOUND + 303;
   int MF_SET_VALUE_RETURN_VIEW = CORE_LOWER_BOUND + 304;

   // PublisherReducers
   int ALL_MATCH_REDUCER = CORE_LOWER_BOUND + 305;
   int ANY_MATCH_REDUCER = CORE_LOWER_BOUND + 306;
   int AND_FINALIZER = CORE_LOWER_BOUND + 307;
   int COLLECT_REDUCER = CORE_LOWER_BOUND + 308;
   int COLLECTOR_FINALIZER = CORE_LOWER_BOUND + 309;
   int COLLECTOR_REDUCER = CORE_LOWER_BOUND + 310;
   int COMBINER_FINALIZER = CORE_LOWER_BOUND + 311;
   int FIND_FIRST_REDUCER_FINALIZER = CORE_LOWER_BOUND + 312;
   int MAX_REDUCER_FINALIZER = CORE_LOWER_BOUND + 313;
   int MIN_REDUCER_FINALIZER = CORE_LOWER_BOUND + 314;
   int NONE_MATCH_REDUCER = CORE_LOWER_BOUND + 315;
   int OR_FINALIZER = CORE_LOWER_BOUND + 316;
   int REDUCE_WITH_IDENTITY_REDUCER = CORE_LOWER_BOUND + 317;
   int REDUCE_WITH_INITIAL_SUPPLIER_REDUCER = CORE_LOWER_BOUND + 318;
   int REDUCE_REDUCER_FINALIZER = CORE_LOWER_BOUND + 319;
   int SUM_REDUCER = CORE_LOWER_BOUND + 320;
   int SUM_FINALIZER = CORE_LOWER_BOUND + 321;
   int TO_ARRAY_FINALIZER = CORE_LOWER_BOUND + 322;
   int TO_ARRAY_REDUCER = CORE_LOWER_BOUND + 323;

   // StreamMarshalling
   int ALWAYS_TRUE_PREDICATE = CORE_LOWER_BOUND + 324;
   int ENTRY_KEY_FUNCTION = CORE_LOWER_BOUND + 325;
   int ENTRY_FUNCTION_ENCODER = CORE_LOWER_BOUND + 326;
   int ENTRY_VALUE_FUNCTION = CORE_LOWER_BOUND + 327;
   int EQUALITY_PREDICATE = CORE_LOWER_BOUND + 328;
   int IDENTITY_FUNCTION = CORE_LOWER_BOUND + 329;
   int KEY_ENTRY_FUNCTION = CORE_LOWER_BOUND + 330;
   int KEY_FUNCTION_ENCODER = CORE_LOWER_BOUND + 331;
   int NON_NULL_PREDICATE = CORE_LOWER_BOUND + 332;

   // Counter range 4000 -> 4199
   int COUNTERS_LOWER_BOUND = 4000;
   int COUNTER_STATE = COUNTERS_LOWER_BOUND;
   int COUNTER_CONFIGURATION = COUNTERS_LOWER_BOUND + 1;
   int COUNTER_TYPE = COUNTERS_LOWER_BOUND + 2;
   int COUNTER_STORAGE = COUNTERS_LOWER_BOUND + 3;
   int COUNTER_FUNCTION_ADD = COUNTERS_LOWER_BOUND + 4;
   int COUNTER_FUNCTION_CAS = COUNTERS_LOWER_BOUND + 5;
   int COUNTER_FUNCTION_CREATE_AND_ADD = COUNTERS_LOWER_BOUND + 6;
   int COUNTER_FUNCTION_CREATE_AND_CAS = COUNTERS_LOWER_BOUND + 7;
   int COUNTER_FUNCTION_CREATE_AND_SET = COUNTERS_LOWER_BOUND + 8;
   int COUNTER_FUNCTION_INITIALIZE_COUNTER = COUNTERS_LOWER_BOUND + 9;
   int COUNTER_FUNCTION_READ = COUNTERS_LOWER_BOUND + 10;
   int COUNTER_FUNCTION_REMOVE = COUNTERS_LOWER_BOUND + 11;
   int COUNTER_FUNCTION_RESET = COUNTERS_LOWER_BOUND + 12;
   int COUNTER_FUNCTION_SET = COUNTERS_LOWER_BOUND + 13;

   // Query range 4200 -> 4399
   int QUERY_LOWER_BOUND = 4200;
// int KNOWN_CLASS_KEY = QUERY_LOWER_BOUND;
   int QUERY_METRICS = QUERY_LOWER_BOUND + 1;
   int LOCAL_QUERY_STATS = QUERY_LOWER_BOUND + 2;
   int LOCAL_INDEX_STATS = QUERY_LOWER_BOUND + 3;
   int INDEX_INFO = QUERY_LOWER_BOUND + 4;
   int INDEX_INFO_ENTRY = QUERY_LOWER_BOUND + 5;
   int SEARCH_STATISTICS = QUERY_LOWER_BOUND + 6;
   int STATS_TASK = QUERY_LOWER_BOUND + 7;
   int CLUSTERED_QUERY_OPERATION = QUERY_LOWER_BOUND + 8;
   int CQ_COMMAND_TYPE = QUERY_LOWER_BOUND + 9;
   int HIBERNATE_POJO_RAW_TYPE_IDENTIFIER = QUERY_LOWER_BOUND + 10;
   int ICKLE_CACHE_EVENT_FILTER_CONVERTER = QUERY_LOWER_BOUND + 11;
   int ICKLE_CONTINOUS_QUERY_CACHE_EVENT_FILTER_CONVERTER = QUERY_LOWER_BOUND + 12;
   int ICKLE_CONTINOUS_QUERY_RESULT = QUERY_LOWER_BOUND + 13;
   int ICKLE_CONTINOUS_QUERY_RESULT_TYPE = QUERY_LOWER_BOUND + 14;
   int ICKLE_DELETE_FUNCTION = QUERY_LOWER_BOUND + 15;
   int ICKLE_FILTER_AND_CONVERTER = QUERY_LOWER_BOUND + 16;
   int ICKLE_FILTER_RESULT = QUERY_LOWER_BOUND + 17;
   int ICKLE_PARSING_RESULT_STATEMENT_TYPE = QUERY_LOWER_BOUND + 18;
   int INDEX_WORKER = QUERY_LOWER_BOUND + 19;
   int NODE_TOP_DOCS = QUERY_LOWER_BOUND + 20;
   int QUERY_DEFINITION = QUERY_LOWER_BOUND + 21;
   int QUERY_RESPONSE = QUERY_LOWER_BOUND + 22;
   int SEGMENTS_CLUSTERED_QUERY_COMMAND = QUERY_LOWER_BOUND + 23;

   // Remote Query range 4400 -> 4599
   int REMOTE_QUERY_LOWER_BOUND = 4400;
   int REMOTE_QUERY_REQUEST = REMOTE_QUERY_LOWER_BOUND;
   int REMOTE_QUERY_RESPONSE = REMOTE_QUERY_LOWER_BOUND + 1;
   int REMOTE_QUERY_ICKLE_FILTER_RESULT = REMOTE_QUERY_LOWER_BOUND + 2;
   int REMOTE_QUERY_ICKLE_CONTINUOUS_QUERY_RESULT = REMOTE_QUERY_LOWER_BOUND + 3;
   int REMOTE_QUERY_ICKLE_BINARY_PROTOBUF_FILTER_AND_CONVERTER = REMOTE_QUERY_LOWER_BOUND + 4;
   int REMOTE_QUERY_ICKLE_CONTINOUS_QUERY_PROTOBUF_CACHE_EVENT_FILTER_CONVERTER = REMOTE_QUERY_LOWER_BOUND + 5;
   int REMOTE_QUERY_ICKLE_PROTOBUF_CACHE_EVENT_FILTER_CONVERTER = REMOTE_QUERY_LOWER_BOUND + 6;
   int REMOTE_QUERY_ICKLE_PROTOBUF_FILTER_AND_CONVERTER = REMOTE_QUERY_LOWER_BOUND + 7;

   // Lucene Directory 4600 -> 4799
   int LUCENE_LOWER_BOUND = 4600;
//   int CHUNK_CACHE_KEY = LUCENE_LOWER_BOUND;
//   int FILE_CACHE_KEY = LUCENE_LOWER_BOUND + 1;
//   int FILE_LIST_CACHE_KEY = LUCENE_LOWER_BOUND + 2;
//   int FILE_METADATA = LUCENE_LOWER_BOUND + 3;
//   int FILE_READ_LOCK_KEY = LUCENE_LOWER_BOUND + 4;
//   int FILE_LIST_CACHE_VALUE = LUCENE_LOWER_BOUND + 5;
   int LUCENE_FIELD_DOC = LUCENE_LOWER_BOUND + 6;
   int LUCENE_SORT = LUCENE_LOWER_BOUND + 7;
   int LUCENE_SORT_FIELD = LUCENE_LOWER_BOUND + 8;
   int LUCENE_SORT_FIELD_TYPE = LUCENE_LOWER_BOUND + 9;
   int LUCENE_TOP_DOCS = LUCENE_LOWER_BOUND + 10;
   int LUCENE_TOP_FIELD_DOCS = LUCENE_LOWER_BOUND + 11;
   int LUCENE_SCORE_DOC = LUCENE_LOWER_BOUND + 12;
   int LUCENE_TOTAL_HITS = LUCENE_LOWER_BOUND + 13;
   int LUCENE_BYTES_REF = LUCENE_LOWER_BOUND + 14;

   // Tasks + Scripting 4800 -> 4999
   int SCRIPTING_LOWER_BOUND = 4800;
   int EXECUTION_MODE = SCRIPTING_LOWER_BOUND;
   int SCRIPT_METADATA = SCRIPTING_LOWER_BOUND + 1;
   int DISTRIBUTED_SERVER_TASK = SCRIPTING_LOWER_BOUND + 2;
   int DISTRIBUTED_SERVER_TASK_PARAMETER = SCRIPTING_LOWER_BOUND + 3;
   int DISTRIBUTED_SERVER_TASK_CONTEXT = SCRIPTING_LOWER_BOUND + 4;
   int DISTRIBUTED_SCRIPT = SCRIPTING_LOWER_BOUND + 5;
   int TASK_EXECUTION_IMPL = SCRIPTING_LOWER_BOUND + 6;

   // Memcached 5000 -> 5099
   int MEMCACHED_LOWER_BOUND = 5000;
   int MEMCACHED_METADATA = MEMCACHED_LOWER_BOUND;

   // RocksDB 5100 -> 5199
   int ROCKSDB_LOWER_BOUND = 5100;
   int ROCKSDB_EXPIRY_BUCKET = ROCKSDB_LOWER_BOUND;
   int ROCKSDB_PERSISTED_METADATA = ROCKSDB_LOWER_BOUND + 1;

   // Event-logger 5200 -> 5299
   int EVENT_LOGGER_LOWER_BOUND = 5200;
   int SERVER_EVENT_IMPL = EVENT_LOGGER_LOWER_BOUND;

   // MultiMap 5300 -> 5399
   int MULTIMAP_LOWER_BOUND = 5300;
   int MULTIMAP_BUCKET = MULTIMAP_LOWER_BOUND;
   int MULTIMAP_LIST_BUCKET = MULTIMAP_LOWER_BOUND + 2;
   int MULTIMAP_HASH_MAP_BUCKET = MULTIMAP_LOWER_BOUND + 3;
   int MULTIMAP_HASH_MAP_BUCKET_ENTRY = MULTIMAP_LOWER_BOUND + 4;
   int MULTIMAP_SET_BUCKET = MULTIMAP_LOWER_BOUND + 5;
   int MULTIMAP_OBJECT_WRAPPER = MULTIMAP_LOWER_BOUND + 6;
   int MULTIMAP_SORTED_SET_BUCKET = MULTIMAP_LOWER_BOUND + 7;
   int MULTIMAP_SCORED_VALUE = MULTIMAP_LOWER_BOUND + 8;
   int MULTIMAP_INDEX_VALUE = MULTIMAP_LOWER_BOUND + 9;
   int MULTIMAP_ADD_MANY_FUNCTION = MULTIMAP_LOWER_BOUND + 10;
   int MULTIMAP_CONTAINS_FUNCTION = MULTIMAP_LOWER_BOUND + 11;
   int MULTIMAP_COUNT_FUNCTION = MULTIMAP_LOWER_BOUND + 12;
   int MULTIMAP_HASH_MAP_PUT_FUNCTION = MULTIMAP_LOWER_BOUND + 13;
   int MULTIMAP_HASH_MAP_KEY_SET_FUNCTION = MULTIMAP_LOWER_BOUND + 14;
   int MULTIMAP_HASH_MAP_VALUES_FUNCTION = MULTIMAP_LOWER_BOUND + 15;
   int MULTIMAP_HASH_MAP_REMOVE_FUNCTION = MULTIMAP_LOWER_BOUND + 16;
   int MULTIMAP_HASH_MAP_REPLACE_FUNCTION = MULTIMAP_LOWER_BOUND + 17;
   int MULTIMAP_GET_FUNCTION = MULTIMAP_LOWER_BOUND + 18;
   int MULTIMAP_INCR_FUNCTION = MULTIMAP_LOWER_BOUND + 19;
   int MULTIMAP_INDEX_FUNCTION = MULTIMAP_LOWER_BOUND + 20;
   int MULTIMAP_INDEX_OF_FUNCTION = MULTIMAP_LOWER_BOUND + 21;
   int MULTIMAP_INDEX_OF_SORTED_SET_FUNCTION = MULTIMAP_LOWER_BOUND + 22;
   int MULTIMAP_INSERT_FUNCTION = MULTIMAP_LOWER_BOUND + 23;
   int MULTIMAP_OFFER_FUNCTION = MULTIMAP_LOWER_BOUND + 24;
   int MULTIMAP_POLL_FUNCTION = MULTIMAP_LOWER_BOUND + 25;
   int MULTIMAP_POP_FUNCTION = MULTIMAP_LOWER_BOUND + 26;
   int MULTIMAP_PUT_FUNCTION = MULTIMAP_LOWER_BOUND + 27;
   int MULTIMAP_REMOVE_FUNCTION = MULTIMAP_LOWER_BOUND + 28;
   int MULTIMAP_REMOVE_COUNT_FUNCTION = MULTIMAP_LOWER_BOUND + 29;
   int MULTIMAP_REMOVE_MANY_FUNCTION = MULTIMAP_LOWER_BOUND + 30;
   int MULTIMAP_REPLACE_LIST_FUNCTION = MULTIMAP_LOWER_BOUND + 31;
   int MULTIMAP_ROTATE_FUNCTION = MULTIMAP_LOWER_BOUND + 32;
   int MULTIMAP_SUBLIST_FUNCTION = MULTIMAP_LOWER_BOUND + 33;
   int MULTIMAP_S_ADD_FUNCTION = MULTIMAP_LOWER_BOUND + 34;
   int MULTIMAP_S_GET_FUNCTION = MULTIMAP_LOWER_BOUND + 35;
   int MULTIMAP_S_M_IS_MEMBER_FUNCTION = MULTIMAP_LOWER_BOUND + 36;
   int MULTIMAP_S_SET_FUNCTION = MULTIMAP_LOWER_BOUND + 37;
   int MULTIMAP_S_POP_FUNCTION = MULTIMAP_LOWER_BOUND + 38;
   int MULTIMAP_S_REMOVE_FUNCTION = MULTIMAP_LOWER_BOUND + 39;
   int MULTIMAP_SET_FUNCTION = MULTIMAP_LOWER_BOUND + 40;
   int MULTIMAP_SCORE_FUNCTION = MULTIMAP_LOWER_BOUND + 41;
   int MULTIMAP_SORTED_SET_AGGREGATE_FUNCTION = MULTIMAP_LOWER_BOUND + 42;
   int MULTIMAP_SORTED_SET_AGGREGATE_FUNCTION_TYPE = MULTIMAP_LOWER_BOUND + 43;
   int MULTIMAP_SORTED_SET_BUCKET_AGGREGATE_FUNCTION = MULTIMAP_LOWER_BOUND + 44;
   int MULTIMAP_SORTED_SET_OPERATION_TYPE = MULTIMAP_LOWER_BOUND + 45;
   int MULTIMAP_SORTED_SET_RANDOM_FUNCTION = MULTIMAP_LOWER_BOUND + 46;
   int MULTIMAP_SUBSET_FUNCTION = MULTIMAP_LOWER_BOUND + 47;
   int MULTIMAP_TRIM_FUNCTION = MULTIMAP_LOWER_BOUND + 48;

   // Server Core 5400 -> 5799
   int SERVER_CORE_LOWER_BOUND = 5400;
   int IGNORED_CACHES = SERVER_CORE_LOWER_BOUND;
   int CACHE_BACKUP_ENTRY = SERVER_CORE_LOWER_BOUND + 1;
   int COUNTER_BACKUP_ENTRY = SERVER_CORE_LOWER_BOUND + 2;
   int IP_FILTER_RULES = SERVER_CORE_LOWER_BOUND + 3;
   int IP_FILTER_RULE = SERVER_CORE_LOWER_BOUND + 4;

   // JDBC Store 5800 -> 5899
   int JDBC_LOWER_BOUND = 5800;
   int JDBC_PERSISTED_METADATA = JDBC_LOWER_BOUND;

   // Spring integration 5900 -> 5999
   int SPRING_LOWER_BOUND = 5900;
   @Deprecated(forRemoval = true, since = "13.0")
   int SPRING_NULL_VALUE = SPRING_LOWER_BOUND;
   int SPRING_SESSION = SPRING_LOWER_BOUND + 1;
   int SPRING_SESSION_ATTRIBUTE = SPRING_LOWER_BOUND + 2;
   int SPRING_SESSION_REMAP = SPRING_LOWER_BOUND + 3;

   // Data distribution metrics 6000 -> 6099
   int DATA_DISTRIBUTION_LOWER_BOUND = 6000;
   int CACHE_DISTRIBUTION_INFO = DATA_DISTRIBUTION_LOWER_BOUND;
   int CLUSTER_DISTRIBUTION_INFO = DATA_DISTRIBUTION_LOWER_BOUND + 1;
   int KEY_DISTRIBUTION_INFO = DATA_DISTRIBUTION_LOWER_BOUND + 2;

   // RESP Objects 6100 -> 6299
   int RESP_LOWER_BOUND = 6100;
   int RESP_HYPER_LOG_LOG = RESP_LOWER_BOUND + 1;
   int RESP_HYPER_LOG_LOG_EXPLICIT = RESP_LOWER_BOUND + 2;
   int RESP_HYPER_LOG_LOG_COMPACT = RESP_LOWER_BOUND + 3;
   int RESP_JSON_BUCKET = RESP_LOWER_BOUND + 4;
   int RESP_COMPOSED_FILTER_CONVERTER = RESP_LOWER_BOUND + 5;
   int RESP_EVENT_LISTENER_CONVERTER = RESP_LOWER_BOUND + 6;
   int RESP_EVENT_LISTENER_KEYS_FILTER = RESP_LOWER_BOUND + 7;
   int RESP_GLOB_MATCH_FILTER_CONVERTER = RESP_LOWER_BOUND + 8;
   int RESP_TYPE_FILTER_CONVERTER = RESP_LOWER_BOUND + 9;
   int RESP_WATCH_TX_EVENT_CONVERTER_EMPTY = RESP_LOWER_BOUND + 10;
   int RESP_JSON_FUNCTION = RESP_LOWER_BOUND + 11;
   int RESP_JSON_ARRAY_APPEND_FUNCTION = RESP_LOWER_BOUND + 12;
   int RESP_JSON_ARRINDEX_FUNCTION = RESP_LOWER_BOUND + 13;
   int RESP_JSON_ARRINSERT_FUNCTION = RESP_LOWER_BOUND + 14;
   int RESP_JSON_ARRPOP_FUNCTION = RESP_LOWER_BOUND + 15;
   int RESP_JSON_ARRTRIM_FUNCTION = RESP_LOWER_BOUND + 16;
   int RESP_JSON_CLEAR_FUNCTION = RESP_LOWER_BOUND + 17;
   int RESP_JSON_DEL_FUNCTION = RESP_LOWER_BOUND + 18;
   int RESP_JSON_GET_FUNCTION = RESP_LOWER_BOUND + 19;
   int RESP_JSON_LEN_ARRAY_FUNCTION = RESP_LOWER_BOUND + 20;
   int RESP_JSON_LEN_OBJ_FUNCTION = RESP_LOWER_BOUND + 21;
   int RESP_JSON_LEN_STRING_FUNCTION = RESP_LOWER_BOUND + 22;
   int RESP_JSON_MERGE_FUNCTION = RESP_LOWER_BOUND + 23;
   int RESP_JSON_NUM_INCR_BY_FUNCTION = RESP_LOWER_BOUND + 24;
   int RESP_JSON_NUM_MULT_FUNCTION = RESP_LOWER_BOUND + 25;
   int RESP_JSON_OBJ_KEY_FUNCTION = RESP_LOWER_BOUND + 26;
   int RESP_JSON_SET_FUNCTION = RESP_LOWER_BOUND + 27;
   int RESP_JSON_STRING_APPEND_FUNCTION = RESP_LOWER_BOUND + 28;
   int RESP_JSON_TOGGLE_FUNCTION = RESP_LOWER_BOUND + 29;
   int RESP_JSON_TYPE_FUNCTION = RESP_LOWER_BOUND + 30;
   int RESP_TYPES = RESP_LOWER_BOUND + 31;
   int RESP_JSON_DEBUG_MEMORY_FUNCTION = RESP_LOWER_BOUND + 32;

   // Clustered Locks 6300 -> 6399
   int CLUSTERED_LOCK_LOWER_BOUND = 6300;
   int CLUSTERED_LOCK_KEY = CLUSTERED_LOCK_LOWER_BOUND;
   int CLUSTERED_LOCK_FILTER = CLUSTERED_LOCK_LOWER_BOUND + 1;
   int CLUSTERED_LOCK_FUNCTION_IS_LOCKED = CLUSTERED_LOCK_LOWER_BOUND + 2;
   int CLUSTERED_LOCK_FUNCTION_LOCK = CLUSTERED_LOCK_LOWER_BOUND + 3;
   int CLUSTERED_LOCK_FUNCTION_UNLOCK = CLUSTERED_LOCK_LOWER_BOUND + 4;
   int CLUSTERED_LOCK_STATE = CLUSTERED_LOCK_LOWER_BOUND + 5;
   int CLUSTERED_LOCK_VALUE = CLUSTERED_LOCK_LOWER_BOUND + 6;

   // Remote Store 6400 -> 6499
   int REMOTE_STORE_LOWER_BOUND = 6400;
   int REMOTE_STORE_ADD = REMOTE_STORE_LOWER_BOUND;
   int REMOTE_STORE_CHECK = REMOTE_STORE_LOWER_BOUND + 1;
   int REMOTE_STORE_DISCONNECT = REMOTE_STORE_LOWER_BOUND + 2;
   int REMOTE_STORE_MIGRATION_TASK = REMOTE_STORE_LOWER_BOUND + 3;
   int REMOTE_STORE_MIGRATION_TASK_ENTRY_WRITER = REMOTE_STORE_LOWER_BOUND + 4;
   int REMOTE_STORE_REMOVED_FILTER = REMOTE_STORE_LOWER_BOUND + 5;

   // Server Core 6500 -> 6599
   int SERVER_LOWER_BOUND = 6500;
   int SERVER_ITERATION_FILTER = SERVER_LOWER_BOUND;
   int SERVER_NETTY_CONNECTION_ADD_TASK = SERVER_LOWER_BOUND + 1;

   // Server HotRod 6600 -> 6799
   int SERVER_HR_LOWER_BOUND = 6600;
   int SERVER_HR_CACHE_XID = SERVER_HR_LOWER_BOUND;
   int SERVER_HR_CONDITION_MARK_ROLLBACK_FUNCTION = SERVER_HR_LOWER_BOUND + 1;
   int SERVER_HR_CHECK_ADDRESS_TASK = SERVER_HR_LOWER_BOUND + 2;
   int SERVER_HR_KEY_VALUE_VERSION_CONVERTER = SERVER_HR_LOWER_BOUND + 3;
   int SERVER_HR_KEY_VALUE_WITH_PREVIOUS_CONVERTER = SERVER_HR_LOWER_BOUND + 4;
   int SERVER_HR_FUNCTION_CREATE_STATE = SERVER_HR_LOWER_BOUND + 5;
   int SERVER_HR_FUNCTION_SET_COMPLETED_TX = SERVER_HR_LOWER_BOUND + 6;
   int SERVER_HR_FUNCTION_DECISION = SERVER_HR_LOWER_BOUND + 7;
   int SERVER_HR_FUNCTION_PREPARED = SERVER_HR_LOWER_BOUND + 8;
   int SERVER_HR_FUNCTION_PREPARING_DECISION = SERVER_HR_LOWER_BOUND + 9;
   int SERVER_HR_MULTI_HOMED_SERVER_ADDRESS = SERVER_HR_LOWER_BOUND + 10;
   int SERVER_HR_MULTI_HOMED_SERVER_ADDRESS_INET = SERVER_HR_LOWER_BOUND + 11;
   int SERVER_HR_SINGLE_HOMED_SERVER_ADDRESS = SERVER_HR_LOWER_BOUND + 12;
   int SERVER_HR_TO_EMPTY_BYTES_KEY_VALUE_FILTER_CONVERTER = SERVER_HR_LOWER_BOUND + 13;
   int SERVER_HR_TX_FORWARD_COMMIT_COMMAND = SERVER_HR_LOWER_BOUND + 14;
   int SERVER_HR_TX_FORWARD_ROLLBACK_COMMAND = SERVER_HR_LOWER_BOUND + 15;
   int SERVER_HR_TX_STATE = SERVER_HR_LOWER_BOUND + 16;
   int SERVER_HR_TX_STATUS = SERVER_HR_LOWER_BOUND + 17;
   int SERVER_HR_XID_PREDICATE = SERVER_HR_LOWER_BOUND + 18;

   // Server Runtime 6800 -> 6899
   int SERVER_RUNTIME_LOWER_BOUND = 6800;
   int SERVER_RUNTIME_EXIT_MODE = SERVER_RUNTIME_LOWER_BOUND + 1;
   int SERVER_RUNTIME_EXIT_STATUS = SERVER_RUNTIME_LOWER_BOUND + 2;
   int SERVER_RUNTIME_SERVER_SHUTDOWN_RUNNABLE = SERVER_RUNTIME_LOWER_BOUND + 3;

   // JCache 6900 -> 6999
   int JCACHE_LOWER_BOUND = 6900;
   int JCACHE_GET_AND_PUT = JCACHE_LOWER_BOUND;
   int JCACHE_GET_AND_REMOVE = JCACHE_LOWER_BOUND + 1;
   int JCACHE_GET_AND_REPLACE = JCACHE_LOWER_BOUND + 2;
   int JCACHE_INVOKE = JCACHE_LOWER_BOUND + 3;
   int JCACHE_MUTABLE_ENTRY_SNAPSHOT = JCACHE_LOWER_BOUND + 4;
   int JCACHE_PUT = JCACHE_LOWER_BOUND + 5;
   int JCACHE_PUT_IF_ABSENT = JCACHE_LOWER_BOUND + 6;
   int JCACHE_READ_WITH_EXPIRY = JCACHE_LOWER_BOUND + 7;
   int JCACHE_REMOVE = JCACHE_LOWER_BOUND + 8;
   int JCACHE_REMOVE_CONDITIONALLY = JCACHE_LOWER_BOUND + 9;
   int JCACHE_REPLACE = JCACHE_LOWER_BOUND + 10;
   int JCACHE_REPLACE_CONDITIONALLY = JCACHE_LOWER_BOUND + 11;

   // Hibernate 7000 -> 7099
   int HIBERNATE_LOWER_BOUND = 7700;
   int HIBERNATE_EVICT_ALL_COMMAND = HIBERNATE_LOWER_BOUND;
   int HIBERNATE_FUTURE_UPDATE = HIBERNATE_LOWER_BOUND + 1;
   int HIBERNATE_INVALIDATE_COMMAND_BEGIN = HIBERNATE_LOWER_BOUND + 2;
   int HIBERNATE_INVALIDATE_COMMAND_END = HIBERNATE_LOWER_BOUND + 3;
   int HIBERNATE_TOMBSTONE = HIBERNATE_LOWER_BOUND + 4;
   int HIBERNATE_TOMBSTONE_EXCLUDE_EMPTY_VERSIONED_ENTRY = HIBERNATE_LOWER_BOUND + 5;
   int HIBERNATE_TOMBSTONE_UPDATE = HIBERNATE_LOWER_BOUND + 6;
   int HIBERNATE_VERSIONED_ENTRY = HIBERNATE_LOWER_BOUND + 7;
   int HIBERNATE_VERSIONED_ENTRY_EXCLUDE_EMPTY_FILTER = HIBERNATE_LOWER_BOUND + 8;
}
