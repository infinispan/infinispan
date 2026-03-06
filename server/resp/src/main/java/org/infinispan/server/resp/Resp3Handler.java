package org.infinispan.server.resp;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.context.Flag;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.multimap.impl.EmbeddedMultimapListCache;
import org.infinispan.multimap.impl.EmbeddedMultimapPairCache;
import org.infinispan.multimap.impl.EmbeddedMultimapSortedSetCache;
import org.infinispan.multimap.impl.EmbeddedSetCache;
import org.infinispan.multimap.impl.HashMapBucket;
import org.infinispan.multimap.impl.ListBucket;
import org.infinispan.multimap.impl.SetBucket;
import org.infinispan.multimap.impl.SortedSetBucket;
import org.infinispan.security.AuthorizationManager;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.json.EmbeddedJsonCache;
import org.infinispan.server.resp.json.JsonBucket;
import org.infinispan.util.concurrent.BlockingManager;

import io.netty.channel.ChannelHandlerContext;

public class Resp3Handler extends Resp3AuthHandler {

   protected AdvancedCache<byte[], byte[]> ignorePreviousValueCache;
   protected EmbeddedMultimapListCache<byte[], byte[]> listMultimap;
   protected EmbeddedMultimapPairCache<byte[], byte[], byte[]> mapMultimap;
   protected EmbeddedSetCache<byte[], byte[]> embeddedSetCache;
   protected EmbeddedMultimapSortedSetCache<byte[], byte[]> sortedSetMultimap;
   protected EmbeddedJsonCache jsonCache;
   protected final ScheduledExecutorService scheduler;
   protected final BlockingManager blockingManager;

   private final MediaType valueMediaType;

   Resp3Handler(RespServer respServer, MediaType valueMediaType, AdvancedCache<byte[], byte[]> cache) {
      // Pass null to super constructor to prevent setCache() from being called before valueMediaType is initialized.
      // We call setCache() manually below after valueMediaType is set.
      super(respServer, null);
      this.valueMediaType = valueMediaType;

      GlobalComponentRegistry gcr = SecurityActions.getGlobalComponentRegistry(respServer.getCacheManager());
      this.scheduler = gcr.getComponent(ScheduledExecutorService.class, KnownComponentNames.TIMEOUT_SCHEDULE_EXECUTOR);
      this.blockingManager = gcr.getComponent(BlockingManager.class);

      if (cache != null) {
         setCache(cache);
      }
   }

   protected Resp3Handler(Resp3Handler delegate) {
      this(delegate.respServer, delegate.valueMediaType, delegate.cache());
   }

   @Override
   public void setCache(AdvancedCache<byte[], byte[]> cache) {
      super.setCache(cache);
      ignorePreviousValueCache = cache.withFlags(Flag.SKIP_CACHE_LOAD, Flag.IGNORE_RETURN_VALUES);
      // All collection maps are stored in Java Objects, so make sure they are encoded as such
      listMultimap = new EmbeddedMultimapListCache<>(cache.withMediaType(MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_OBJECT.withClassType(ListBucket.class)));
      mapMultimap = new EmbeddedMultimapPairCache<>(cache.withMediaType(MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_OBJECT.withClassType(HashMapBucket.class)));
      embeddedSetCache = new EmbeddedSetCache<>(cache.withMediaType(MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_OBJECT.withClassType(SetBucket.class)));
      sortedSetMultimap = new EmbeddedMultimapSortedSetCache<>(cache.withMediaType(MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_OBJECT.withClassType(SortedSetBucket.class)));
      jsonCache = new EmbeddedJsonCache(cache.withMediaType(MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_OBJECT.withClassType(JsonBucket.class)));
   }

   public EmbeddedJsonCache getJsonCache() {
      return jsonCache;
   }

   public EmbeddedMultimapListCache<byte[], byte[]> getListMultimap() {
      return listMultimap;
   }

   public EmbeddedMultimapPairCache<byte[], byte[], byte[]> getHashMapMultimap() {
      return mapMultimap;
   }

   public EmbeddedSetCache<byte[], byte[]> getEmbeddedSetCache() {
      return embeddedSetCache;
   }

   public EmbeddedMultimapSortedSetCache<byte[], byte[]> getSortedSeMultimap() {
      return sortedSetMultimap;
   }

   public ScheduledExecutorService getScheduler() {
      return scheduler;
   }

   public BlockingManager getBlockingManager() {
      return blockingManager;
   }

   @Override
   protected CompletionStage<RespRequestHandler> actualHandleRequest(ChannelHandlerContext ctx, RespCommand type,
         List<byte[]> arguments) {
      if (type instanceof Resp3Command resp3Command) {
         return resp3Command.perform(this, ctx, arguments);
      } else {
         return super.actualHandleRequest(ctx, type, arguments);
      }
   }

   public AdvancedCache<byte[], byte[]> ignorePreviousValuesCache() {
      return ignorePreviousValueCache;
   }

   public CompletionStage<RespRequestHandler> delegate(ChannelHandlerContext ctx,
         RespCommand command,
         List<byte[]> arguments) {
      return super.actualHandleRequest(ctx, command, arguments);
   }

   public void checkPermission(AuthorizationPermission authorizationPermission) {
      AuthorizationManager authorizationManager = cache().getAuthorizationManager();
      if (authorizationManager != null) {
         authorizationManager.checkPermission(authorizationPermission);
      }
   }
}
