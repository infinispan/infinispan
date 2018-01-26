package org.infinispan.server.hotrod;

import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.server.hotrod.logging.Log;
import org.infinispan.server.hotrod.multimap.MultimapGetWithMetadataResponse;
import org.infinispan.server.hotrod.multimap.MultimapResponse;
import org.infinispan.util.logging.LogFactory;

import io.netty.channel.Channel;

class MultimapRequestProcessor extends BaseRequestProcessor {
   private static final Log log = LogFactory.getLog(MultimapRequestProcessor.class, Log.class);
   private static final boolean trace = log.isTraceEnabled();

   MultimapRequestProcessor(Channel channel, Executor executor) {
      super(channel, executor);
   }

   void get(CacheDecodeContext cdc) {
      if (trace) {
         log.trace("Call get");
      }
      WrappedByteArray keyWrappped = new WrappedByteArray(cdc.getKey());
      cdc.multimap().get(keyWrappped).whenComplete(
            (result, throwable) -> handleGet(cdc, result, throwable));
   }

   private void handleGet(CacheDecodeContext cdc, Collection<WrappedByteArray> result, Throwable throwable) {
      if (throwable != null) {
         writeException(cdc, throwable);
      } else try {
         OperationStatus status = OperationStatus.Success;
         if (result.isEmpty()) {
            status = OperationStatus.KeyDoesNotExist;
         }
         writeResponse(new MultimapResponse<Collection<byte[]>>(cdc.header, HotRodOperation.GET_MULTIMAP, status,
               mapToCollectionOfByteArrays(result)));
      } catch (Throwable t2) {
         writeException(cdc, t2);
      }
   }

   void getWithMetadata(CacheDecodeContext cdc) {
      if (trace) {
         log.trace("Call getWithMetadata");
      }
      WrappedByteArray keyWrappped = new WrappedByteArray(cdc.getKey());
      cdc.multimap().getEntry(keyWrappped).whenComplete((entry, throwable) -> handleGetWithMetadata(cdc, entry, throwable));
   }

   private void handleGetWithMetadata(CacheDecodeContext cdc, Optional<CacheEntry<WrappedByteArray, Collection<WrappedByteArray>>> entry, Throwable throwable) {
      if (throwable != null) {
         writeException(cdc, throwable);
      } else try {
         OperationStatus status = OperationStatus.KeyDoesNotExist;
         CacheEntry<WrappedByteArray, Collection<WrappedByteArray>> ce = null;
         Collection<byte[]> result = null;
         if (entry.isPresent()) {
            status = OperationStatus.Success;
            ce = entry.get();
            result = mapToCollectionOfByteArrays(ce.getValue());
         }
         writeResponse(new MultimapGetWithMetadataResponse(cdc.header, status, ce, result));
      } catch (Throwable t2) {
         writeException(cdc, t2);
      }
   }

   private Set<byte[]> mapToCollectionOfByteArrays(Collection<WrappedByteArray> result) {
      return result.stream().map(WrappedByteArray::getBytes).collect(Collectors.toSet());
   }

   void put(CacheDecodeContext cdc) {
      if (trace) {
         log.trace("Call put");
      }
      WrappedByteArray keyWrappped = new WrappedByteArray(cdc.getKey());
      WrappedByteArray valueWrapped = new WrappedByteArray(cdc.getValue());
      cdc.multimap().put(keyWrappped, valueWrapped).whenComplete(success(cdc, HotRodOperation.PUT_MULTIMAP));
   }

   void removeKey(CacheDecodeContext cdc) {
      if (trace) {
         log.trace("Call removeKey");
      }
      WrappedByteArray keyWrappped = new WrappedByteArray(cdc.getKey());
      cdc.multimap().remove(keyWrappped).whenComplete(success(cdc, HotRodOperation.REMOVE_MULTIMAP));
   }

   void removeEntry(CacheDecodeContext cdc) {
      log.trace("Call removeEntry");
      WrappedByteArray keyWrappped = new WrappedByteArray(cdc.getKey());
      WrappedByteArray valueWrapped = new WrappedByteArray(cdc.getValue());
      cdc.multimap().remove(keyWrappped, valueWrapped).whenComplete(success(cdc, HotRodOperation.REMOVE_ENTRY_MULTIMAP));
   }

   void size(CacheDecodeContext cdc) {
      log.trace("Call size");
      cdc.multimap().size().whenComplete(success(cdc, HotRodOperation.SIZE_MULTIMAP));
   }

   void containsEntry(CacheDecodeContext cdc) {
      log.trace("Call containsEntry");
      WrappedByteArray keyWrappped = new WrappedByteArray(cdc.getKey());
      WrappedByteArray valueWrapped = new WrappedByteArray(cdc.getValue());
      cdc.multimap().containsEntry(keyWrappped, valueWrapped).whenComplete(success(cdc, HotRodOperation.CONTAINS_ENTRY_MULTIMAP));
   }

   void containsKey(CacheDecodeContext cdc) {
      log.trace("Call containsKey");
      WrappedByteArray keyWrappped = new WrappedByteArray(cdc.getKey());
      cdc.multimap().containsKey(keyWrappped).whenComplete(success(cdc, HotRodOperation.CONTAINS_KEY_MULTIMAP));
   }

   void containsValue(CacheDecodeContext cdc) {
      log.trace("Call containsValue");
      WrappedByteArray valueWrapped = new WrappedByteArray(cdc.getValue());
      cdc.multimap().containsValue(valueWrapped).whenComplete(success(cdc, HotRodOperation.CONTAINS_VALUE_MULTIMAP));
   }

   private <T> BiConsumer<T, Throwable> success(CacheDecodeContext cdc, HotRodOperation operation) {
      return (result, throwable) -> {
         if (throwable != null) {
            writeException(cdc, throwable);
         } else {
            writeResponse(new MultimapResponse<>(cdc.header, operation, OperationStatus.Success, result));
         }
      };
   }
}
