package org.infinispan.client.hotrod.impl;

/**
 * // TODO: Document this
 *
 * @author mmarkus
 * @since 4.1
 */
public interface HotrodConstants {

   public static final short REQUEST_MAGIC = 0xA0;
   public static final short RESPONSE_MAGIC = 0xA1;

   public static final byte HOTROD_VERSION = 10;

   //requests
   public static final byte PUT_REQUEST = 0x01;
   public static final byte GET_REQUEST = 0x03;
   public static final byte PUT_IF_ABSENT_REQUEST = 0x05;
   public static final byte REPLACE_REQUEST = 0x07;
   public static final byte REPLACE_IF_UNMODIFIED_REQUEST = 0x09;
   public static final byte REMOVE_REQUEST = 0x0B;
   public static final byte REMOVE_IF_UNMODIFIED_REQUEST = 0x0D;
   public static final byte CONTAINS_KEY_REQUEST = 0x0F;
   public static final byte GET_WITH_VERSION = 0x11;
   public static final byte CLEAR_REQUEST = 0x13;
   public static final byte STATS_REQUEST = 0x15;
   public static final byte PING_REQUEST = 0x17;


   //responses
   public static final byte PUT_RESPONSE = 0x02;
   public static final byte GET_RESPONSE = 0x04;
   public static final byte PUT_IF_ABSENT_RESPONSE = 0x06;
   public static final byte REPLACE_RESPONSE = 0x08;
   public static final byte REPLACE_IF_UNMODIFIED_RESPONSE = 0x0A;
   public static final byte REMOVE_RESPONSE = 0x0C;
   public static final byte REMOVE_IF_UNMODIFIED_RESPONSE = 0x0E;
   public static final byte CONTAINS_KEY_RESPONSE = 0x10;
   public static final byte GET_WITH_VERSION_RESPONSE = 0x12;
   public static final byte CLEAR_RESPONSE = 0x14;
   public static final byte STATS_RESPONSE = 0x16;
   public static final byte PING_RESPONSE = 0x18;
   public static final byte ERROR_RESPONSE = 0x50;

   //response status
   public static final byte NO_ERROR_STATUS = 0x00;
   public static final int INVALID_MAGIC_OR_MESSAGE_ID_STATUS = 0x81;
   public static final int REQUEST_PARSING_ERROR_STATUS = 0x84;
   public static final byte NOT_PUT_REMOVED_REPLACED_STATUS = 0x01;
   public static final int UNKNOWN_COMMAND_STATUS = 0x82;
   public static final int SERVER_ERROR_STATUS = 0x85;
   public static final int KEY_DOES_NOT_EXIST_STATUS = 0x02;
   public static final int UNKNOWN_VERSION_STATUS = 0x83;
   public static final int COMMAND_TIMEOUT_STATUS = 0x86;


   public static final byte CLIENT_INTELLIGENCE_BASIC = 0x01;
   public static final byte CLIENT_INTELLIGENCE_TOPOLOGY_AWARE = 0x02;
   public static final byte CLIENT_INTELLIGENCE_HASH_DISTRIBUTION_AWARE = 0x03;
}
