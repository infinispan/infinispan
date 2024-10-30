package org.infinispan.server.resp;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.context.Flag;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.multimap.impl.EmbeddedMultimapListCache;
import org.infinispan.multimap.impl.EmbeddedMultimapPairCache;
import org.infinispan.multimap.impl.EmbeddedMultimapSortedSetCache;
import org.infinispan.multimap.impl.EmbeddedSetCache;
import org.infinispan.security.AuthorizationManager;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.json.EmbeddedJsonCache;
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
      super(respServer, cache);
      this.valueMediaType = valueMediaType;

      GlobalComponentRegistry gcr = SecurityActions.getGlobalComponentRegistry(cache.getCacheManager());
      this.scheduler = gcr.getComponent(ScheduledExecutorService.class, KnownComponentNames.TIMEOUT_SCHEDULE_EXECUTOR);
      this.blockingManager = gcr.getComponent(BlockingManager.class);
   }

   @Override
   public void setCache(AdvancedCache<byte[], byte[]> cache) {
      super.setCache(cache);
      ignorePreviousValueCache = cache.withFlags(Flag.SKIP_CACHE_LOAD, Flag.IGNORE_RETURN_VALUES);
      Cache toMultimap = cache.withMediaType(MediaType.APPLICATION_OCTET_STREAM, valueMediaType);
      listMultimap = new EmbeddedMultimapListCache<>(toMultimap);
      mapMultimap = new EmbeddedMultimapPairCache<>(toMultimap);
      embeddedSetCache = new EmbeddedSetCache<>(toMultimap);
      sortedSetMultimap = new EmbeddedMultimapSortedSetCache<>(toMultimap);
      jsonCache = new EmbeddedJsonCache(toMultimap);
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
      if (type instanceof Resp3Command) {
         Resp3Command resp3Command = (Resp3Command) type;
         return resp3Command.perform(this, ctx, arguments);
      }
      return super.actualHandleRequest(ctx, type, arguments);
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
      AuthorizationManager authorizationManager = cache.getAuthorizationManager();
      if (authorizationManager != null) {
         authorizationManager.checkPermission(authorizationPermission);
      }
   }
}
