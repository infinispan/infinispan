package org.infinispan.server.hotrod;

/**
 * Defines constants defined by Hot Rod specifications.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public interface HotRodConstants {
   byte VERSION_20 = HotRodVersion.HOTROD_20.getVersion();
   byte VERSION_21 = HotRodVersion.HOTROD_21.getVersion();
   byte VERSION_22 = HotRodVersion.HOTROD_22.getVersion();
   byte VERSION_24 = HotRodVersion.HOTROD_24.getVersion();
   byte VERSION_26 = HotRodVersion.HOTROD_26.getVersion();
   byte VERSION_28 = HotRodVersion.HOTROD_28.getVersion();
   byte VERSION_30 = HotRodVersion.HOTROD_30.getVersion();
   byte VERSION_31 = HotRodVersion.HOTROD_31.getVersion();
   byte VERSION_40 = HotRodVersion.HOTROD_40.getVersion();

   //requests
   byte PUT_REQUEST = 0x01;
   byte GET_REQUEST = 0x03;
   byte PUT_IF_ABSENT_REQUEST = 0x05;
   byte REPLACE_REQUEST = 0x07;
   byte REPLACE_IF_UNMODIFIED_REQUEST = 0x09;
   byte REMOVE_REQUEST = 0x0B;
   byte REMOVE_IF_UNMODIFIED_REQUEST = 0x0D;
   byte CONTAINS_KEY_REQUEST = 0x0F;
   byte GET_WITH_VERSION = 0x11;
   byte CLEAR_REQUEST = 0x13;
   byte STATS_REQUEST = 0x15;
   byte PING_REQUEST = 0x17;
   byte BULK_GET_REQUEST = 0x19;
   byte GET_WITH_METADATA = 0x1B;
   byte BULK_GET_KEYS_REQUEST = 0x1D;
   byte QUERY_REQUEST = 0x1F;
   byte AUTH_MECH_LIST_REQUEST = 0x21;
   byte AUTH_REQUEST = 0x23;
   byte ADD_CLIENT_LISTENER_REQUEST = 0x25;
   byte REMOVE_CLIENT_LISTENER_REQUEST = 0x27;
   byte SIZE_REQUEST = 0x29;
   byte EXEC_REQUEST = 0x2B;
   byte PUT_ALL_REQUEST = 0x2D;
   byte GET_ALL_REQUEST = 0x2F;
   byte ITERATION_START_REQUEST = 0x31;
   byte ITERATION_NEXT_REQUEST = 0x33;
   byte ITERATION_END_REQUEST = 0x35;
   byte GET_STREAM_REQUEST = 0x37;
   byte PUT_STREAM_REQUEST = 0x39;

   byte PREPARE_TX = 0x3B;
   byte COMMIT_TX = 0x3D;
   byte ROLLBACK_TX = 0x3F;
   byte ADD_BLOOM_FILTER_NEAR_CACHE_LISTENER_REQUEST = 0x41;
   byte UPDATE_BLOOM_FILTER_REQUEST = 0x43;
   byte FORGET_TX = 0x79;
   byte FETCH_TX_RECOVERY = 0x7B;
   byte PREPARE_TX_2 = 0x7D;

   byte COUNTER_CREATE_REQUEST = 0x4B;
   byte COUNTER_GET_CONFIGURATION_REQUEST = 0x4D;
   byte COUNTER_IS_DEFINED_REQUEST = 0x4F;
   byte COUNTER_ADD_AND_GET_REQUEST = 0x52;
   byte COUNTER_RESET_REQUEST = 0x54;
   byte COUNTER_GET_REQUEST = 0x56;
   byte COUNTER_CAS_REQUEST = 0x58;
   byte COUNTER_ADD_LISTENER_REQUEST = 0x5A;
   byte COUNTER_REMOVE_LISTENER_REQUEST = 0x5C;
   byte COUNTER_REMOVE_REQUEST = 0x5E;
   byte COUNTER_GET_NAMES_REQUEST = 0x64;

   byte GET_MULTIMAP_REQUEST = 0x67;
   byte GET_MULTIMAP_WITH_METADATA_REQUEST = 0x69;
   byte PUT_MULTIMAP_REQUEST = 0x6B;
   byte REMOVE_KEY_MULTIMAP_REQUEST = 0x6D;
   byte REMOVE_ENTRY_MULTIMAP_REQUEST = 0x6F;
   byte SIZE_MULTIMAP_REQUEST = 0x71;
   byte CONTAINS_ENTRY_REQUEST = 0x73;
   byte CONTAINS_KEY_MULTIMAP_REQUEST = 0x75;
   byte CONTAINS_VALUE_MULTIMAP_REQUEST = 0x77;

   //0x79 FORGET_TX
   //0x7B FETCH_TX_RECOVERY
   //0x7D PREPARE_TX_2
}
