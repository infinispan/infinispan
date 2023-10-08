package org.infinispan.server.resp.commands.tx;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryExpired;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilter;
import org.infinispan.notifications.cachelistener.filter.EventType;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.commands.TransactionResp3Command;
import org.infinispan.server.resp.filter.EventListenerKeysFilter;
import org.infinispan.server.resp.meta.ClientMetadata;
import org.infinispan.server.resp.serialization.ResponseWriter;
import org.infinispan.server.resp.tx.RespTransactionHandler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.util.AttributeKey;

/**
 * WATCH
 * <p>
 * Installs a clustered listener to watch for the given <code>key</code>s. The listener receives events for creation,
 * updates, and expiration.
 * <p>
 * The watch instance is local to a single {@link ChannelHandlerContext}. To remove the listeners, the same context
 * needs to execute the operation. There is no way to remove a single specific watcher. All listeners deregister during
 * an {@link EXEC}, {@link UNWATCH}, DISCARD, transaction abort, or closed channel.
 * <p>
 * Since a listener is bound to a single connection, this ensures that another client does not affect each other's
 * transactions safeguards.
 *
 * @author José Bolina
 * @see <a href="https://redis.io/commands/watch/">WATCH</a>
 * @since 15.0
 */
public class WATCH extends RespCommand implements Resp3Command, TransactionResp3Command {

   static final AttributeKey<List<TxKeysListener>> WATCHER_KEY = AttributeKey.newInstance("watchers");

   public WATCH() {
      super(-2, 1, -1, 1);
   }

   @Override
   public long aclMask() {
      return AclCategory.FAST | AclCategory.TRANSACTION;
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx, List<byte[]> arguments) {
      AdvancedCache<byte[], byte[]> cache = handler.cache();
      TxKeysListener listener = new TxKeysListener(arguments.size());

      CacheEventFilter<Object, Object> filter = new EventListenerKeysFilter(arguments.stream());
      CompletionStage<Void> cs = cache.addListenerAsync(listener, filter, new TxEventConverterEmpty())
            .thenAccept(ignore -> register(ctx, listener))
            .thenAccept(ignore -> {
               ClientMetadata metadata = handler.respServer().metadataRepository().client();
               metadata.incrementWatchingClients();
               metadata.recordWatchedKeys(arguments.size());
            });
      return handler.stageToReturn(cs, ctx, ResponseWriter.OK);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(RespTransactionHandler handler, ChannelHandlerContext ctx, List<byte[]> arguments) {
      handler.writer().customError("WATCH inside MULTI is not allowed");
      return handler.myStage();
   }

   public void register(ChannelHandlerContext ctx, WATCH.TxKeysListener listener) {
      List<WATCH.TxKeysListener> watchers = ctx.channel().attr(WATCHER_KEY).get();
      if (watchers == null) {
         watchers = new ArrayList<>();
         ctx.channel().attr(WATCHER_KEY).set(watchers);
      }

      watchers.add(listener);
   }

   @Listener(clustered = true)
   public static class TxKeysListener {
      private final AtomicBoolean hasEvent = new AtomicBoolean(false);
      private final int numberOfKeys;

      public TxKeysListener(int numberOfKeys) {
         this.numberOfKeys = numberOfKeys;
      }

      @CacheEntryCreated
      @CacheEntryModified
      @CacheEntryExpired
      @CacheEntryRemoved
      public CompletionStage<Void> onEvent(CacheEntryEvent<Object, Object> ignore) {
         hasEvent.set(true);
         return CompletableFutures.completedNull();
      }

      public boolean hasSeenEvents() {
         return hasEvent.get();
      }

      public int getNumberOfKeys() {
         return numberOfKeys;
      }
   }

   @ProtoTypeId(ProtoStreamTypeIds.RESP_WATCH_TX_EVENT_CONVERTER_EMPTY)
   public static class TxEventConverterEmpty implements CacheEventConverter<Object, Object, Object> {

      @Override
      public Object convert(Object key, Object oldValue, Metadata oldMetadata, Object newValue, Metadata newMetadata, EventType eventType) {
         // We don't care about the event value.
         return null;
      }
   }
}
