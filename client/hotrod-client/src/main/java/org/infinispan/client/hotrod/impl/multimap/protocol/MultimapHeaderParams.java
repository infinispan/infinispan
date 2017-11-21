package org.infinispan.client.hotrod.impl.multimap.protocol;

import org.infinispan.client.hotrod.impl.protocol.HeaderParams;

/**
 * Hot Rod request header parameters for Multimap
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
public class MultimapHeaderParams extends HeaderParams {
   protected short toOpRespCode(short opCode) {
      switch (opCode) {
         case MultimapHotRodConstants.GET_MULTIMAP_REQUEST:
            return MultimapHotRodConstants.GET_MULTIMAP_RESPONSE;
         case MultimapHotRodConstants.GET_MULTIMAP_WITH_METADATA_REQUEST:
            return MultimapHotRodConstants.GET_MULTIMAP_WITH_METADATA_RESPONSE;
         case MultimapHotRodConstants.PUT_MULTIMAP_REQUEST:
            return MultimapHotRodConstants.PUT_MULTIMAP_RESPONSE;
         case MultimapHotRodConstants.REMOVE_KEY_MULTIMAP_REQUEST:
            return MultimapHotRodConstants.REMOVE_KEY_MULTIMAP_RESPONSE;
         case MultimapHotRodConstants.REMOVE_ENTRY_MULTIMAP_REQUEST:
            return MultimapHotRodConstants.REMOVE_ENTRY_MULTIMAP_RESPONSE;
         case MultimapHotRodConstants.SIZE_MULTIMAP_REQUEST:
            return MultimapHotRodConstants.SIZE_MULTIMAP_RESPONSE;
         case MultimapHotRodConstants.CONTAINS_ENTRY_REQUEST:
            return MultimapHotRodConstants.CONTAINS_ENTRY_RESPONSE;
         case MultimapHotRodConstants.CONTAINS_KEY_MULTIMAP_REQUEST:
            return MultimapHotRodConstants.CONTAINS_KEY_MULTIMAP_RESPONSE;
         case MultimapHotRodConstants.CONTAINS_VALUE_MULTIMAP_REQUEST:
            return MultimapHotRodConstants.CONTAINS_VALUE_MULTIMAP_RESPONSE;
         default:
            throw new IllegalStateException("Unknown operation code: " + opCode);
      }
   }
}
