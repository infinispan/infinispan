package org.infinispan.client.hotrod.impl.multimap.protocol;

/**
 * Multimap hotrod constants
 *
 * @author Katia Aresti
 * @since 9.2
 */
public interface MultimapHotRodConstants {

   // requests
   byte GET_MULTIMAP_REQUEST = 0x67;
   byte GET_MULTIMAP_WITH_METADATA_REQUEST = 0x69;
   byte PUT_MULTIMAP_REQUEST = 0x6B;
   byte REMOVE_KEY_MULTIMAP_REQUEST = 0x6D;
   byte REMOVE_ENTRY_MULTIMAP_REQUEST = 0x6F;
   byte SIZE_MULTIMAP_REQUEST = 0x71;
   byte CONTAINS_ENTRY_REQUEST = 0x73;
   byte CONTAINS_KEY_MULTIMAP_REQUEST = 0x75;
   short CONTAINS_VALUE_MULTIMAP_REQUEST = 0x77;

   // responses
   byte GET_MULTIMAP_RESPONSE = 0x68;
   byte GET_MULTIMAP_WITH_METADATA_RESPONSE = 0x6A;
   byte PUT_MULTIMAP_RESPONSE = 0x6C;
   byte REMOVE_KEY_MULTIMAP_RESPONSE = 0x6E;
   byte REMOVE_ENTRY_MULTIMAP_RESPONSE = 0x70;
   byte SIZE_MULTIMAP_RESPONSE = 0x72;
   byte CONTAINS_ENTRY_RESPONSE = 0x74;
   short CONTAINS_KEY_MULTIMAP_RESPONSE = 0x76;
   short CONTAINS_VALUE_MULTIMAP_RESPONSE = 0x78;
}
