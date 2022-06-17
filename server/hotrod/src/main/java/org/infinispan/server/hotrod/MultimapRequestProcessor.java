package org.infinispan.server.hotrod;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;

import javax.security.auth.Subject;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.server.hotrod.logging.Log;
import org.infinispan.util.logging.LogFactory;

import io.netty.channel.Channel;

class MultimapRequestProcessor extends BaseRequestProcessor {
   private static final Log log = LogFactory.getLog(MultimapRequestProcessor.class, Log.class);

   MultimapRequestProcessor(Channel channel, Executor executor, HotRodServer server) {
      super(channel, executor, server);
   }

   void get(HotRodHeader header, Subject subject, byte[] key, boolean supportsDuplicates) {
      if (log.isTraceEnabled()) {
         log.trace("Call get");
      }
      server.multimap(header, subject, supportsDuplicates).get(key).whenComplete(
            (result, throwable) -> handleGet(header, result, throwable));
   }

   private void handleGet(HotRodHeader header, Collection<byte[]> result, Throwable throwable) {
      if (throwable != null) {
         writeException(header, throwable);
      } else try {
         OperationStatus status = OperationStatus.Success;
         if (result.isEmpty()) {
            writeNotExist(header);
            return;
         }
         writeResponse(header, header.encoder().multimapCollectionResponse(header, server, channel, status, result));
      } catch (Throwable t2) {
         writeException(header, t2);
      }
   }

   void getWithMetadata(HotRodHeader header, Subject subject, byte[] key, boolean supportsDuplicates) {
      if (log.isTraceEnabled()) {
         log.trace("Call getWithMetadata");
      }
      server.multimap(header, subject, supportsDuplicates).getEntry(key).whenComplete((entry, throwable) -> handleGetWithMetadata(header, entry, throwable));
   }

   private void handleGetWithMetadata(HotRodHeader header, Optional<CacheEntry<byte[], Collection<byte[]>>> entry, Throwable throwable) {
      if (throwable != null) {
         writeException(header, throwable);
      } else try {
         if (!entry.isPresent()) {
            writeNotExist(header);
            return;
         }
         OperationStatus status = OperationStatus.Success;
         CacheEntry<byte[], Collection<byte[]>> ce = entry.get();
         writeResponse(header, header.encoder().multimapEntryResponse(header, server, channel, status, ce));
      } catch (Throwable t2) {
         writeException(header, t2);
      }
   }

   void put(HotRodHeader header, Subject subject, byte[] key, byte[] value, boolean supportsDuplicates) {
      if (log.isTraceEnabled()) {
         log.trace("Call put");
      }
      server.multimap(header, subject, supportsDuplicates).put(key, value).whenComplete((result, throwable) -> {
         if (throwable != null) {
            writeException(header, throwable);
         } else {
            writeResponse(header, header.encoder().emptyResponse(header, server, channel, OperationStatus.Success));
         }
      });
   }

   void removeKey(HotRodHeader header, Subject subject, byte[] key, boolean supportsDuplicates) {
      if (log.isTraceEnabled()) {
         log.trace("Call removeKey");
      }
      server.multimap(header, subject, supportsDuplicates).remove(key).whenComplete(handleBoolean(header));
   }

   void removeEntry(HotRodHeader header, Subject subject, byte[] key, byte[] value, boolean supportsDuplicates) {
      log.trace("Call removeEntry");
      server.multimap(header, subject, supportsDuplicates).remove(key, value).whenComplete(handleBoolean(header));
   }

   void size(HotRodHeader header, Subject subject, boolean supportsDuplicates) {
      log.trace("Call size");
      server.multimap(header, subject, supportsDuplicates).size().whenComplete((result, throwable) -> {
         if (throwable != null) {
            writeException(header, throwable);
         } else {
            writeResponse(header, header.encoder().unsignedLongResponse(header, server, channel, result));
         }
      });
   }

   void containsEntry(HotRodHeader header, Subject subject, byte[] key, byte[] value, boolean supportsDuplicates) {
      log.trace("Call containsEntry");
      server.multimap(header, subject, supportsDuplicates).containsEntry(key, value).whenComplete(handleBoolean(header));
   }

   void containsKey(HotRodHeader header, Subject subject, byte[] key, boolean supportsDuplicates) {
      log.trace("Call containsKey");
      server.multimap(header, subject, supportsDuplicates).containsKey(key).whenComplete(handleBoolean(header));
   }

   void containsValue(HotRodHeader header, Subject subject, byte[] value, boolean supportsDuplicates) {
      log.trace("Call containsValue");
      server.multimap(header, subject, supportsDuplicates).containsValue(value).whenComplete(handleBoolean(header));
   }

   private BiConsumer<Boolean, Throwable> handleBoolean(HotRodHeader header) {
      return (result, throwable) -> {
         if (throwable != null) {
            writeException(header, throwable);
         } else {
            writeResponse(header, header.encoder().booleanResponse(header, server, channel, result));
         }
      };
   }
}
