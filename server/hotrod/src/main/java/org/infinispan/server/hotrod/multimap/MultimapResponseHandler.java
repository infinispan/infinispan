package org.infinispan.server.hotrod.multimap;

import java.util.Collection;

import org.infinispan.server.hotrod.MetadataUtils;
import org.infinispan.server.hotrod.OperationStatus;
import org.infinispan.server.hotrod.Response;
import org.infinispan.server.hotrod.transport.ExtendedByteBuf;

import io.netty.buffer.ByteBuf;

/**
 * Utility class used to write responses of Multimap Operations
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
public final class MultimapResponseHandler {
   private MultimapResponseHandler() {
   }

   /**
    * Write operation response using the given channel buffer
    *
    * @param response, operation response
    * @param buf, the buffer
    */
   public static void handle(Response response, ByteBuf buf) {
      switch (response.getOperation()) {
         case PUT_MULTIMAP: {
            break;
         }
         case GET_MULTIMAP: {
            MultimapResponse<Collection<byte[]>> r = (MultimapResponse) response;
            if (r.getStatus() == OperationStatus.Success) {
               ExtendedByteBuf.writeUnsignedInt(r.getResult().size(), buf);
               r.getResult().forEach(v -> ExtendedByteBuf.writeRangedBytes(v, buf));
            }
            break;
         }
         case GET_MULTIMAP_WITH_METADATA: {
            MultimapGetWithMetadataResponse r = (MultimapGetWithMetadataResponse) response;
            if (r.getStatus() == OperationStatus.Success) {
               MetadataUtils.writeMetadata(r.getLifespan(), r.getMaxIdle(), r.getCreated(), r.getLastUsed(), r.getDataVersion(), buf);
               ExtendedByteBuf.writeUnsignedInt(r.getResult().size(), buf);
               r.getResult().forEach(v -> ExtendedByteBuf.writeRangedBytes(v, buf));
            }
            break;
         }
         case REMOVE_MULTIMAP: {
            MultimapResponse<Boolean> r = (MultimapResponse) response;
            if (r.getStatus() == OperationStatus.Success) {
               ExtendedByteBuf.writeUnsignedInt(r.getResult().booleanValue() ? 1 : 0, buf);
            }
            break;
         }
         case REMOVE_ENTRY_MULTIMAP: {
            MultimapResponse<Boolean> r = (MultimapResponse) response;
            if (r.getStatus() == OperationStatus.Success) {
               ExtendedByteBuf.writeUnsignedInt(r.getResult().booleanValue() ? 1 : 0, buf);
            }
            break;
         }
         case SIZE_MULTIMAP: {
            MultimapResponse<Long> r = (MultimapResponse) response;
            if (r.getStatus() == OperationStatus.Success) {
               ExtendedByteBuf.writeUnsignedLong(r.getResult(), buf);
            }
            break;
         }
         case CONTAINS_ENTRY_MULTIMAP: {
            MultimapResponse<Boolean> r = (MultimapResponse) response;
            if (r.getStatus() == OperationStatus.Success) {
               ExtendedByteBuf.writeUnsignedInt(r.getResult().booleanValue() ? 1 : 0, buf);
            }
            break;
         }
         case CONTAINS_KEY_MULTIMAP: {
            MultimapResponse<Boolean> r = (MultimapResponse) response;
            if (r.getStatus() == OperationStatus.Success) {
               ExtendedByteBuf.writeUnsignedInt(r.getResult().booleanValue() ? 1 : 0, buf);
            }
            break;
         }
         case CONTAINS_VALUE_MULTIMAP: {
            MultimapResponse<Boolean> r = (MultimapResponse) response;
            if (r.getStatus() == OperationStatus.Success) {
               ExtendedByteBuf.writeUnsignedInt(r.getResult().booleanValue() ? 1 : 0, buf);
            }
            break;
         }
         default:
            throw new UnsupportedOperationException(response.toString());
      }
   }
}
