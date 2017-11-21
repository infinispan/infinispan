package org.infinispan.server.hotrod.multimap;

import static org.infinispan.server.hotrod.ResponseWriting.writeResponse;

import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.multimap.impl.EmbeddedMultimapCache;
import org.infinispan.server.hotrod.CacheDecodeContext;
import org.infinispan.server.hotrod.HotRodHeader;
import org.infinispan.server.hotrod.HotRodOperation;
import org.infinispan.server.hotrod.OperationStatus;
import org.infinispan.server.hotrod.Response;
import org.infinispan.server.hotrod.logging.Log;
import org.infinispan.util.logging.LogFactory;

import io.netty.channel.ChannelHandlerContext;

/**
 * Wrapper over {@link CacheDecodeContext} for multimap
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
public final class MultimapCacheDecodeContext {
   static final Log log = LogFactory.getLog(MultimapCacheDecodeContext.class, Log.class);
   private final EmbeddedMultimapCache<WrappedByteArray, WrappedByteArray> multimapCache;
   private final CacheDecodeContext cacheDecodeContext;
   private final HotRodHeader header;

   public MultimapCacheDecodeContext(AdvancedCache<byte[], byte[]> cache, CacheDecodeContext cacheDecodeContext) {
      this.multimapCache = new EmbeddedMultimapCache(cache);
      this.cacheDecodeContext = cacheDecodeContext;
      this.header = cacheDecodeContext.getHeader();
   }

   public void read(ChannelHandlerContext ctx) throws Exception {
      switch (header.getOp()) {
         case PUT_MULTIMAP:
            writeResponse(cacheDecodeContext, ctx.channel(), this.put());
            break;
         case GET_MULTIMAP:
            writeResponse(cacheDecodeContext, ctx.channel(), this.get());
            break;
         case GET_MULTIMAP_WITH_METADATA:
            writeResponse(cacheDecodeContext, ctx.channel(), this.getWithMetadata());
            break;
         case REMOVE_MULTIMAP:
            writeResponse(cacheDecodeContext, ctx.channel(), this.removeKey());
            break;
         case REMOVE_ENTRY_MULTIMAP:
            writeResponse(cacheDecodeContext, ctx.channel(), this.removeEntry());
            break;
         case SIZE_MULTIMAP:
            writeResponse(cacheDecodeContext, ctx.channel(), this.size());
            break;
         case CONTAINS_ENTRY_MULTIMAP:
            writeResponse(cacheDecodeContext, ctx.channel(), this.containsEntry());
            break;
         case CONTAINS_KEY_MULTIMAP:
            writeResponse(cacheDecodeContext, ctx.channel(), this.containsKey());
            break;
         case CONTAINS_VALUE_MULTIMAP:
            writeResponse(cacheDecodeContext, ctx.channel(), this.containsValue());
            break;
         default:
            throw new IllegalArgumentException("Unsupported operation invoked: " + header.getOp());
      }
   }

   public Response get() {
      log.trace("Call get");
      WrappedByteArray keyWrappped = new WrappedByteArray(cacheDecodeContext.getKey());
      Collection<WrappedByteArray> result = multimapCache.get(keyWrappped).join();
      OperationStatus status = OperationStatus.Success;
      if (result.isEmpty()) {
         status = OperationStatus.KeyDoesNotExist;
      }
      return new MultimapResponse<Collection<byte[]>>(header, HotRodOperation.GET_MULTIMAP, status,
            mapToCollectionOfByteArrays(result));
   }

   public Response getWithMetadata() {
      log.trace("Call getWithMetadata");
      WrappedByteArray keyWrappped = new WrappedByteArray(cacheDecodeContext.getKey());
      Optional<CacheEntry<WrappedByteArray, Collection<WrappedByteArray>>> entry = multimapCache.getEntry(keyWrappped).join();
      OperationStatus status = OperationStatus.KeyDoesNotExist;
      CacheEntry<WrappedByteArray, Collection<WrappedByteArray>> ce = null;
      Collection<byte[]> result = null;
      if (entry.isPresent()) {
         status = OperationStatus.Success;
         ce = entry.get();
         result = mapToCollectionOfByteArrays(ce.getValue());
      }
      return new MultimapGetWithMetadataResponse(header, status, ce, result);
   }

   private Set<byte[]> mapToCollectionOfByteArrays(Collection<WrappedByteArray> result) {
      return result.stream().map(WrappedByteArray::getBytes).collect(Collectors.toSet());
   }

   public Response put() {
      log.trace("Call put");
      WrappedByteArray keyWrappped = new WrappedByteArray(cacheDecodeContext.getKey());
      WrappedByteArray valueWrapped = new WrappedByteArray(cacheDecodeContext.getValue());
      multimapCache.put(keyWrappped, valueWrapped).join();
      return new MultimapResponse<>(header, HotRodOperation.PUT_MULTIMAP, OperationStatus.Success, null);
   }

   public Response removeKey() {
      log.trace("Call removeKey");
      WrappedByteArray keyWrappped = new WrappedByteArray(cacheDecodeContext.getKey());
      Boolean result = multimapCache.remove(keyWrappped).join();
      return new MultimapResponse<>(header, HotRodOperation.REMOVE_MULTIMAP, OperationStatus.Success, result);
   }

   public Response removeEntry() {
      log.trace("Call removeEntry");
      WrappedByteArray keyWrappped = new WrappedByteArray(cacheDecodeContext.getKey());
      WrappedByteArray valueWrapped = new WrappedByteArray(cacheDecodeContext.getValue());
      Boolean result = multimapCache.remove(keyWrappped, valueWrapped).join();
      return new MultimapResponse<>(header, HotRodOperation.REMOVE_ENTRY_MULTIMAP, OperationStatus.Success, result);
   }

   public Response size() {
      log.trace("Call size");
      Long result = multimapCache.size().join();
      return new MultimapResponse<>(header, HotRodOperation.SIZE_MULTIMAP, OperationStatus.Success, result);
   }

   public Response containsEntry() {
      log.trace("Call containsEntry");
      WrappedByteArray keyWrappped = new WrappedByteArray(cacheDecodeContext.getKey());
      WrappedByteArray valueWrapped = new WrappedByteArray(cacheDecodeContext.getValue());
      Boolean result = multimapCache.containsEntry(keyWrappped, valueWrapped).join();
      return new MultimapResponse<>(header, HotRodOperation.CONTAINS_ENTRY_MULTIMAP, OperationStatus.Success, result);
   }

   public Response containsKey() {
      log.trace("Call containsKey");
      WrappedByteArray keyWrappped = new WrappedByteArray(cacheDecodeContext.getKey());
      Boolean result = multimapCache.containsKey(keyWrappped).join();
      return new MultimapResponse<>(header, HotRodOperation.CONTAINS_KEY_MULTIMAP, OperationStatus.Success, result);
   }

   public Response containsValue() {
      log.trace("Call containsValue");
      WrappedByteArray valueWrapped = new WrappedByteArray(cacheDecodeContext.getValue());
      Boolean result = multimapCache.containsValue(valueWrapped).join();
      return new MultimapResponse<>(header, HotRodOperation.CONTAINS_VALUE_MULTIMAP, OperationStatus.Success, result);
   }
}
