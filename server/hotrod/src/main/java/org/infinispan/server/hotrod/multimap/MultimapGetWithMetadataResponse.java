package org.infinispan.server.hotrod.multimap;

import static org.infinispan.server.hotrod.MetadataUtils.extractCreated;
import static org.infinispan.server.hotrod.MetadataUtils.extractLastUsed;
import static org.infinispan.server.hotrod.MetadataUtils.extractLifespan;
import static org.infinispan.server.hotrod.MetadataUtils.extractMaxIdle;
import static org.infinispan.server.hotrod.MetadataUtils.extractVersion;

import java.util.Collection;

import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.util.Util;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.server.hotrod.HotRodHeader;
import org.infinispan.server.hotrod.HotRodOperation;
import org.infinispan.server.hotrod.OperationStatus;

/**
 * Get response with metadata
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
public class MultimapGetWithMetadataResponse extends MultimapResponse<Collection<byte[]>> {
   protected final long dataVersion;
   protected final long created;
   protected final int lifespan;
   protected final long lastUsed;
   protected final int maxIdle;

   public MultimapGetWithMetadataResponse(HotRodHeader header,
                                          OperationStatus operationStatus,
                                          CacheEntry<WrappedByteArray, Collection<WrappedByteArray>> ce,
                                          Collection<byte[]> result) {
      super(header, HotRodOperation.GET_MULTIMAP_WITH_METADATA, operationStatus, result);
      this.dataVersion = extractVersion(ce);
      this.created = extractCreated(ce);
      this.lifespan = extractLifespan(ce);
      this.lastUsed = extractLastUsed(ce);
      this.maxIdle = extractMaxIdle(ce);
   }

   @Override
   public String toString() {
      StringBuilder sb = new StringBuilder(super.toString());
      sb.append("MultimapGetWithMetadataResponse{")
        .append("version=").append(version)
        .append(", messageId=").append(messageId)
        .append(", cacheName=").append(cacheName)
        .append(", clientIntel=").append(clientIntel)
        .append(", operation=").append(operation)
        .append(", status=").append(status)
        .append(", topologyId=").append(topologyId)
        .append(", value=");
      if (getResult() != null) {
         sb.append("[");
         getResult().forEach(v -> sb.append(Util.printArray(v, true)));
         sb.append("]");
      } else {
         sb.append("null");
      }
      sb.append(", dataVersion=").append(dataVersion)
        .append(", created=").append(created)
        .append(", lifespan=").append(lifespan)
        .append(", lastUsed=").append(lastUsed)
        .append(", maxIdle=").append(maxIdle)
        .append("}");

      return sb.toString();
   }

   public int getLifespan() {
      return lifespan;
   }

   public int getMaxIdle() {
      return maxIdle;
   }

   public long getCreated() {
      return created;
   }

   public long getLastUsed() {
      return lastUsed;
   }

   public long getDataVersion() {
      return dataVersion;
   }
}
