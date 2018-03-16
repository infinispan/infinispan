package org.infinispan.server.hotrod;

import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import javax.security.auth.Subject;

import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.server.hotrod.logging.Log;
import org.infinispan.util.logging.LogFactory;

import io.netty.channel.Channel;

class MultimapRequestProcessor extends BaseRequestProcessor {
   private static final Log log = LogFactory.getLog(MultimapRequestProcessor.class, Log.class);
   private static final boolean trace = log.isTraceEnabled();

   MultimapRequestProcessor(Channel channel, Executor executor, HotRodServer server) {
      super(channel, executor, server);
   }

   void get(HotRodHeader header, Subject subject, byte[] key) {
      if (trace) {
         log.trace("Call get");
      }
      WrappedByteArray keyWrappped = new WrappedByteArray(key);
      server.multimap(header, subject).get(keyWrappped).whenComplete(
            (result, throwable) -> handleGet(header, result, throwable));
   }

   private void handleGet(HotRodHeader header, Collection<WrappedByteArray> result, Throwable throwable) {
      if (throwable != null) {
         writeException(header, throwable);
      } else try {
         OperationStatus status = OperationStatus.Success;
         if (result.isEmpty()) {
            status = OperationStatus.KeyDoesNotExist;
         }
         writeResponse(header, header.encoder().multimapCollectionResponse(header, server, channel.alloc(), status,
               mapToCollectionOfByteArrays(result)));
      } catch (Throwable t2) {
         writeException(header, t2);
      }
   }

   void getWithMetadata(HotRodHeader header, Subject subject, byte[] key) {
      if (trace) {
         log.trace("Call getWithMetadata");
      }
      WrappedByteArray keyWrappped = new WrappedByteArray(key);
      server.multimap(header, subject).getEntry(keyWrappped).whenComplete((entry, throwable) -> handleGetWithMetadata(header, entry, throwable));
   }

   private void handleGetWithMetadata(HotRodHeader header, Optional<CacheEntry<WrappedByteArray, Collection<WrappedByteArray>>> entry, Throwable throwable) {
      if (throwable != null) {
         writeException(header, throwable);
      } else try {
         OperationStatus status = OperationStatus.KeyDoesNotExist;
         CacheEntry<WrappedByteArray, Collection<WrappedByteArray>> ce = null;
         Collection<byte[]> result = null;
         if (entry.isPresent()) {
            status = OperationStatus.Success;
            ce = entry.get();
            result = mapToCollectionOfByteArrays(ce.getValue());
         }
         writeResponse(header, header.encoder().multimapEntryResponse(header, server, channel.alloc(), status, ce, result));
      } catch (Throwable t2) {
         writeException(header, t2);
      }
   }

   private Set<byte[]> mapToCollectionOfByteArrays(Collection<WrappedByteArray> result) {
      return result.stream().map(WrappedByteArray::getBytes).collect(Collectors.toSet());
   }

   void put(HotRodHeader header, Subject subject, byte[] key, byte[] value) {
      if (trace) {
         log.trace("Call put");
      }
      WrappedByteArray keyWrappped = new WrappedByteArray(key);
      WrappedByteArray valueWrapped = new WrappedByteArray(value);
      server.multimap(header, subject).put(keyWrappped, valueWrapped).whenComplete((result, throwable) -> {
         if (throwable != null) {
            writeException(header, throwable);
         } else {
            writeResponse(header, header.encoder().emptyResponse(header, server, channel.alloc(), OperationStatus.Success));
         }
      });
   }

   void removeKey(HotRodHeader header, Subject subject, byte[] key) {
      if (trace) {
         log.trace("Call removeKey");
      }
      WrappedByteArray keyWrappped = new WrappedByteArray(key);
      server.multimap(header, subject).remove(keyWrappped).whenComplete(handleBoolean(header));
   }

   void removeEntry(HotRodHeader header, Subject subject, byte[] key, byte[] value) {
      log.trace("Call removeEntry");
      WrappedByteArray keyWrappped = new WrappedByteArray(key);
      WrappedByteArray valueWrapped = new WrappedByteArray(value);
      server.multimap(header, subject).remove(keyWrappped, valueWrapped).whenComplete(handleBoolean(header));
   }

   void size(HotRodHeader header, Subject subject) {
      log.trace("Call size");
      server.multimap(header, subject).size().whenComplete((result, throwable) -> {
         if (throwable != null) {
            writeException(header, throwable);
         } else {
            writeResponse(header, header.encoder().unsignedLongResponse(header, server, channel.alloc(), result));
         }
      });
   }

   void containsEntry(HotRodHeader header, Subject subject, byte[] key, byte[] value) {
      log.trace("Call containsEntry");
      WrappedByteArray keyWrappped = new WrappedByteArray(key);
      WrappedByteArray valueWrapped = new WrappedByteArray(value);
      server.multimap(header, subject).containsEntry(keyWrappped, valueWrapped).whenComplete(handleBoolean(header));
   }

   void containsKey(HotRodHeader header, Subject subject, byte[] key) {
      log.trace("Call containsKey");
      WrappedByteArray keyWrappped = new WrappedByteArray(key);
      server.multimap(header, subject).containsKey(keyWrappped).whenComplete(handleBoolean(header));
   }

   void containsValue(HotRodHeader header, Subject subject, byte[] value) {
      log.trace("Call containsValue");
      WrappedByteArray valueWrapped = new WrappedByteArray(value);
      server.multimap(header, subject).containsValue(valueWrapped).whenComplete(handleBoolean(header));
   }

   private BiConsumer<Boolean, Throwable> handleBoolean(HotRodHeader header) {
      return (result, throwable) -> {
         if (throwable != null) {
            writeException(header, throwable);
         } else {
            writeResponse(header, header.encoder().booleanResponse(header, server, channel.alloc(), result));
         }
      };
   }
}
