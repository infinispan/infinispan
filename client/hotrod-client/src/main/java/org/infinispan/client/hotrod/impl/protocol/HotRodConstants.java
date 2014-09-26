package org.infinispan.client.hotrod.impl.protocol;

import java.nio.charset.Charset;

/**
 * Defines constants defined by Hot Rod specifications.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public interface HotRodConstants {

   static final short REQUEST_MAGIC = 0xA0;
   static final short RESPONSE_MAGIC = 0xA1;

   static final byte VERSION_10 = 10;
   static final byte VERSION_11 = 11;
   static final byte VERSION_12 = 12;
   static final byte VERSION_13 = 13;
   static final byte VERSION_20 = 20;

   //requests
   static final byte PUT_REQUEST = 0x01;
   static final byte GET_REQUEST = 0x03;
   static final byte PUT_IF_ABSENT_REQUEST = 0x05;
   static final byte REPLACE_REQUEST = 0x07;
   static final byte REPLACE_IF_UNMODIFIED_REQUEST = 0x09;
   static final byte REMOVE_REQUEST = 0x0B;
   static final byte REMOVE_IF_UNMODIFIED_REQUEST = 0x0D;
   static final byte CONTAINS_KEY_REQUEST = 0x0F;
   static final byte GET_WITH_VERSION = 0x11;
   static final byte CLEAR_REQUEST = 0x13;
   static final byte STATS_REQUEST = 0x15;
   static final byte PING_REQUEST = 0x17;
   static final byte BULK_GET_REQUEST = 0x19;
   static final byte GET_WITH_METADATA = 0x1B;
   static final byte BULK_GET_KEYS_REQUEST = 0x1D;
   static final byte QUERY_REQUEST = 0x1F;
   static final byte AUTH_MECH_LIST_REQUEST = 0x21;
   static final byte AUTH_REQUEST = 0x23;
   static final byte ADD_CLIENT_LISTENER_REQUEST = 0x25;
   static final byte REMOVE_CLIENT_LISTENER_REQUEST = 0x27;
   static final byte SIZE_REQUEST = 0x29;

   //responses
   static final byte PUT_RESPONSE = 0x02;
   static final byte GET_RESPONSE = 0x04;
   static final byte PUT_IF_ABSENT_RESPONSE = 0x06;
   static final byte REPLACE_RESPONSE = 0x08;
   static final byte REPLACE_IF_UNMODIFIED_RESPONSE = 0x0A;
   static final byte REMOVE_RESPONSE = 0x0C;
   static final byte REMOVE_IF_UNMODIFIED_RESPONSE = 0x0E;
   static final byte CONTAINS_KEY_RESPONSE = 0x10;
   static final byte GET_WITH_VERSION_RESPONSE = 0x12;
   static final byte CLEAR_RESPONSE = 0x14;
   static final byte STATS_RESPONSE = 0x16;
   static final byte PING_RESPONSE = 0x18;
   static final byte BULK_GET_RESPONSE = 0x1A;
   static final byte GET_WITH_METADATA_RESPONSE = 0x1C;
   static final byte BULK_GET_KEYS_RESPONSE = 0x1E;
   static final byte QUERY_RESPONSE = 0x20;
   static final byte AUTH_MECH_LIST_RESPONSE = 0x22;
   static final byte AUTH_RESPONSE = 0x24;
   static final byte ADD_CLIENT_LISTENER_RESPONSE = 0x26;
   static final byte REMOVE_CLIENT_LISTENER_RESPONSE = 0x28;
   static final byte SIZE_RESPONSE = 0x2A;
   static final byte ERROR_RESPONSE = 0x50;
   static final byte CACHE_ENTRY_CREATED_EVENT_RESPONSE = 0x60;
   static final byte CACHE_ENTRY_MODIFIED_EVENT_RESPONSE = 0x61;
   static final byte CACHE_ENTRY_REMOVED_EVENT_RESPONSE = 0x62;

   //response status
   static final byte NO_ERROR_STATUS = 0x00;
   static final int INVALID_MAGIC_OR_MESSAGE_ID_STATUS = 0x81;
   static final int REQUEST_PARSING_ERROR_STATUS = 0x84;
   static final byte NOT_PUT_REMOVED_REPLACED_STATUS = 0x01;
   static final int UNKNOWN_COMMAND_STATUS = 0x82;
   static final int SERVER_ERROR_STATUS = 0x85;
   static final int KEY_DOES_NOT_EXIST_STATUS = 0x02;
   static final int UNKNOWN_VERSION_STATUS = 0x83;
   static final int COMMAND_TIMEOUT_STATUS = 0x86;
   static final int NODE_SUSPECTED = 0x87;
   static final int ILLEGAL_LIFECYCLE_STATE = 0x88;
   static final int SUCCESS_WITH_PREVIOUS = 0x03;
   static final int NOT_EXECUTED_WITH_PREVIOUS = 0x04;


   static final byte CLIENT_INTELLIGENCE_BASIC = 0x01;
   static final byte CLIENT_INTELLIGENCE_TOPOLOGY_AWARE = 0x02;
   static final byte CLIENT_INTELLIGENCE_HASH_DISTRIBUTION_AWARE = 0x03;
   Charset HOTROD_STRING_CHARSET = Charset.forName("UTF-8");

   static final byte[] DEFAULT_CACHE_NAME_BYTES = new byte[]{};

   static final byte INFINITE_LIFESPAN = 0x01;
   static final byte INFINITE_MAXIDLE = 0x02;
}
