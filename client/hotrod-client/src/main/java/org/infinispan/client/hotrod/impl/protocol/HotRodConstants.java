package org.infinispan.client.hotrod.impl.protocol;

import java.nio.charset.Charset;

/**
 * Defines constants defined by Hot Rod specifications.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public interface HotRodConstants {

   short REQUEST_MAGIC = 0xA0;
   short RESPONSE_MAGIC = 0xA1;

   byte VERSION_10 = 10;
   byte VERSION_11 = 11;
   byte VERSION_12 = 12;
   byte VERSION_13 = 13;
   byte VERSION_20 = 20;
   byte VERSION_21 = 21;
   byte VERSION_22 = 22;
   byte VERSION_23 = 23;
   byte VERSION_24 = 24;
   byte VERSION_25 = 25;
   byte VERSION_26 = 26;
   byte VERSION_27 = 27;

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

   //responses
   byte PUT_RESPONSE = 0x02;
   byte GET_RESPONSE = 0x04;
   byte PUT_IF_ABSENT_RESPONSE = 0x06;
   byte REPLACE_RESPONSE = 0x08;
   byte REPLACE_IF_UNMODIFIED_RESPONSE = 0x0A;
   byte REMOVE_RESPONSE = 0x0C;
   byte REMOVE_IF_UNMODIFIED_RESPONSE = 0x0E;
   byte CONTAINS_KEY_RESPONSE = 0x10;
   byte GET_WITH_VERSION_RESPONSE = 0x12;
   byte CLEAR_RESPONSE = 0x14;
   byte STATS_RESPONSE = 0x16;
   byte PING_RESPONSE = 0x18;
   byte BULK_GET_RESPONSE = 0x1A;
   byte GET_WITH_METADATA_RESPONSE = 0x1C;
   byte BULK_GET_KEYS_RESPONSE = 0x1E;
   byte QUERY_RESPONSE = 0x20;
   byte AUTH_MECH_LIST_RESPONSE = 0x22;
   byte AUTH_RESPONSE = 0x24;
   byte ADD_CLIENT_LISTENER_RESPONSE = 0x26;
   byte REMOVE_CLIENT_LISTENER_RESPONSE = 0x28;
   byte SIZE_RESPONSE = 0x2A;
   byte EXEC_RESPONSE = 0x2C;
   byte PUT_ALL_RESPONSE = 0x2E;
   byte GET_ALL_RESPONSE = 0x30;
   byte ITERATION_START_RESPONSE = 0x32;
   byte ITERATION_NEXT_RESPONSE = 0x34;
   byte ITERATION_END_RESPONSE = 0x36;
   byte GET_STREAM_RESPONSE = 0x38;
   byte PUT_STREAM_RESPONSE = 0x3A;
   byte ERROR_RESPONSE = 0x50;
   byte CACHE_ENTRY_CREATED_EVENT_RESPONSE = 0x60;
   byte CACHE_ENTRY_MODIFIED_EVENT_RESPONSE = 0x61;
   byte CACHE_ENTRY_REMOVED_EVENT_RESPONSE = 0x62;
   byte CACHE_ENTRY_EXPIRED_EVENT_RESPONSE = 0x63;
   byte COUNTER_CREATE_RESPONSE = 0x4C;
   byte COUNTER_GET_CONFIGURATION_RESPONSE = 0x4E;
   byte COUNTER_IS_DEFINED_RESPONSE = 0x51;
   byte COUNTER_ADD_AND_GET_RESPONSE = 0x53;
   byte COUNTER_RESET_RESPONSE = 0x55;
   byte COUNTER_GET_RESPONSE = 0x57;
   byte COUNTER_CAS_RESPONSE = 0x59;
   byte COUNTER_ADD_LISTENER_RESPONSE = 0x5B;
   byte COUNTER_REMOVE_LISTENER_RESPONSE = 0x5D;
   byte COUNTER_REMOVE_RESPONSE = 0x5F;
   byte COUNTER_GET_NAMES_RESPONSE = 0x65;
   byte COUNTER_EVENT_RESPONSE = 0x66;

   //response status
   byte NO_ERROR_STATUS = 0x00;
   byte NOT_PUT_REMOVED_REPLACED_STATUS = 0x01;
   int KEY_DOES_NOT_EXIST_STATUS = 0x02;
   int SUCCESS_WITH_PREVIOUS = 0x03;
   int NOT_EXECUTED_WITH_PREVIOUS = 0x04;
   int INVALID_ITERATION = 0x05;
   byte NO_ERROR_STATUS_COMPAT = 0x06;
   byte SUCCESS_WITH_PREVIOUS_COMPAT = 0x07;
   byte NOT_EXECUTED_WITH_PREVIOUS_COMPAT = 0x08;

   int INVALID_MAGIC_OR_MESSAGE_ID_STATUS = 0x81;
   int REQUEST_PARSING_ERROR_STATUS = 0x84;
   int UNKNOWN_COMMAND_STATUS = 0x82;
   int SERVER_ERROR_STATUS = 0x85;
   int UNKNOWN_VERSION_STATUS = 0x83;
   int COMMAND_TIMEOUT_STATUS = 0x86;
   int NODE_SUSPECTED = 0x87;
   int ILLEGAL_LIFECYCLE_STATE = 0x88;

   @Deprecated
   /**
    * @deprecated use {@link org.infinispan.client.hotrod.configuration.ClientIntelligence#BASIC}
    * instead
    */
   byte CLIENT_INTELLIGENCE_BASIC = 0x01;
   @Deprecated
   /**
    * @deprecated use {@link org.infinispan.client.hotrod.configuration.ClientIntelligence#TOPOLOGY_AWARE}
    * instead
    */
   byte CLIENT_INTELLIGENCE_TOPOLOGY_AWARE = 0x02;
   @Deprecated
   /**
    * @deprecated use {@link org.infinispan.client.hotrod.configuration.ClientIntelligence#HASH_DISTRIBUTION_AWARE}
    * instead
    */
   byte CLIENT_INTELLIGENCE_HASH_DISTRIBUTION_AWARE = 0x03;
   Charset HOTROD_STRING_CHARSET = Charset.forName("UTF-8");

   byte[] DEFAULT_CACHE_NAME_BYTES = new byte[]{};

   byte INFINITE_LIFESPAN = 0x01;
   byte INFINITE_MAXIDLE = 0x02;

   int DEFAULT_CACHE_TOPOLOGY = -1;
   int SWITCH_CLUSTER_TOPOLOGY = -2;

   static boolean isSuccess(short status) {
      return status == NO_ERROR_STATUS
         || status == NO_ERROR_STATUS_COMPAT
         || status == SUCCESS_WITH_PREVIOUS
         || status == SUCCESS_WITH_PREVIOUS_COMPAT;
   }

   static boolean isNotExecuted(short status) {
      return status == NOT_PUT_REMOVED_REPLACED_STATUS
         || status == NOT_EXECUTED_WITH_PREVIOUS
         || status == NOT_EXECUTED_WITH_PREVIOUS_COMPAT;
   }

   static boolean isNotExist(short status) {
      return status == KEY_DOES_NOT_EXIST_STATUS;
   }

   static boolean hasPrevious(short status) {
      return status == SUCCESS_WITH_PREVIOUS
         || status == SUCCESS_WITH_PREVIOUS_COMPAT
         || status == NOT_EXECUTED_WITH_PREVIOUS
         || status == NOT_EXECUTED_WITH_PREVIOUS_COMPAT;
   }

   static boolean hasCompatibility(short status) {
      return status == NO_ERROR_STATUS_COMPAT
         || status == SUCCESS_WITH_PREVIOUS_COMPAT
         || status == NOT_EXECUTED_WITH_PREVIOUS_COMPAT;
   }

   static boolean isInvalidIteration(short status) {
      return status == INVALID_ITERATION;
   }

}
