package org.infinispan.remoting.inboundhandler;

import static org.infinispan.factories.KnownComponentNames.BLOCKING_EXECUTOR;
import static org.infinispan.util.logging.Log.CLUSTER;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.GlobalRpcCommand;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.IllegalLifecycleStateException;
import org.infinispan.configuration.ConfigurationManager;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifier;
import org.infinispan.notifications.cachemanagerlistener.annotation.CacheStopped;
import org.infinispan.notifications.cachemanagerlistener.event.CacheStoppedEvent;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.HeartBeatCommand;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.XSiteReplicateCommand;

/**
 * {@link org.infinispan.remoting.inboundhandler.InboundInvocationHandler} implementation that handles all the {@link
 * org.infinispan.commands.ReplicableCommand}.
 * <p/>
 * This component handles the {@link org.infinispan.commands.ReplicableCommand} from local and remote site. The remote
 * site {@link org.infinispan.commands.ReplicableCommand} are sent to the {@link org.infinispan.xsite.BackupReceiver} to
 * be handled.
 * <p/>
 * Also, the non-{@link org.infinispan.commands.remote.CacheRpcCommand} are processed directly and the {@link
 * org.infinispan.commands.remote.CacheRpcCommand} are processed in the cache's {@link
 * org.infinispan.remoting.inboundhandler.PerCacheInboundInvocationHandler} implementation.
 *
 * @author Pedro Ruivo
 * @since 7.1
 */
@Listener
@Scope(Scopes.GLOBAL)
public class GlobalInboundInvocationHandler implements InboundInvocationHandler {

   private static final Log log = LogFactory.getLog(GlobalInboundInvocationHandler.class);

   // TODO: To be removed with https://issues.redhat.com/browse/ISPN-11483
   @Inject @ComponentName(BLOCKING_EXECUTOR)
   ExecutorService blockingExecutor;
   @Inject GlobalComponentRegistry globalComponentRegistry;
   @Inject CacheManagerNotifier managerNotifier;

   private final Map<RemoteSiteCache, LocalSiteCache> localCachesMap = new ConcurrentHashMap<>();

   private static Response shuttingDownResponse() {
      return CacheNotFoundResponse.INSTANCE;
   }

   private static ExceptionResponse exceptionHandlingCommand(Throwable throwable) {
      if (throwable instanceof Exception) {
         return new ExceptionResponse(((Exception) throwable));
      } else {
         return new ExceptionResponse(new CacheException("Problems invoking command.", throwable));
      }
   }

   @Start
   public void start() {
      managerNotifier.addListener(this);
   }

   @Stop
   public void stop() {
      managerNotifier.removeListener(this);
   }

   @CacheStopped
   public void cacheStopped(CacheStoppedEvent event) {
      ByteString cacheName = ByteString.fromString(event.getCacheName());
      localCachesMap.entrySet().removeIf(entry -> entry.getValue().cacheName.equals(cacheName));
   }

   @Override
   public void handleFromCluster(Address origin, ReplicableCommand command, Reply reply, DeliverOrder order) {
      command.setOrigin(origin);
      try {
         if (command.getCommandId() == HeartBeatCommand.COMMAND_ID) {
            reply.reply(null);
         } else if (command instanceof CacheRpcCommand) {
            handleCacheRpcCommand(origin, (CacheRpcCommand) command, reply, order);
         } else {
            handleReplicableCommand(origin, command, reply, order);
         }
      } catch (Throwable t) {
         CLUSTER.exceptionHandlingCommand(command, t);
         reply.reply(exceptionHandlingCommand(t));
      }
   }

   @Override
   public void handleFromRemoteSite(String origin, XSiteReplicateCommand<?> command, Reply reply, DeliverOrder order) {
      if (log.isTraceEnabled()) {
         log.tracef("Handling command %s from remote site %s", command, origin);
      }

      LocalSiteCache localCache = findLocalCacheForRemoteSite(origin, command.getCacheName());
      if (localCache == null) {
         reply.reply(new ExceptionResponse(log.xsiteCacheNotFound(origin, command.getCacheName())));
         return;
      } else if (localCache.local) {
         reply.reply(new ExceptionResponse(log.xsiteInLocalCache(origin, localCache.cacheName)));
         return;
      }
      ComponentRegistry cr = globalComponentRegistry.getNamedComponentRegistry(localCache.cacheName);
      if (cr == null) {
         reply.reply(new ExceptionResponse(log.xsiteCacheNotStarted(origin, localCache.cacheName)));
         return;
      }
      PerCacheInboundInvocationHandler handler = cr.getPerCacheInboundInvocationHandler();
      assert handler != null;
      handler.registerXSiteCommandReceiver();
      command.performInLocalSite(cr, order.preserveOrder()).whenComplete(new ResponseConsumer(command, reply));
   }

   private void handleCacheRpcCommand(Address origin, CacheRpcCommand command, Reply reply, DeliverOrder mode) {
      if (log.isTraceEnabled()) {
         log.tracef("Attempting to execute CacheRpcCommand: %s [sender=%s]", command, origin);
      }
      ByteString cacheName = command.getCacheName();
      ComponentRegistry cr = globalComponentRegistry.getNamedComponentRegistry(cacheName);

      if (cr == null) {
         if (log.isTraceEnabled()) {
            log.tracef("Silently ignoring that %s cache is not defined", cacheName);
         }
         reply.reply(CacheNotFoundResponse.INSTANCE);
         return;
      }
      CommandsFactory commandsFactory = cr.getCommandsFactory();
      // initialize this command with components specific to the intended cache instance
      commandsFactory.initializeReplicableCommand(command, true);
      PerCacheInboundInvocationHandler handler = cr.getPerCacheInboundInvocationHandler();
      handler.handle(command, reply, mode);
   }

   private void handleReplicableCommand(Address origin, ReplicableCommand command, Reply reply, DeliverOrder order) {
      if (log.isTraceEnabled()) {
         log.tracef("Attempting to execute non-CacheRpcCommand: %s [sender=%s]", command, origin);
      }
      Runnable runnable = new ReplicableCommandRunner(command, reply, globalComponentRegistry, order.preserveOrder());
      if (order.preserveOrder() || !command.canBlock()) {
         //we must/can run in this thread
         runnable.run();
      } else {
         blockingExecutor.execute(runnable);
      }
   }

   /**
    * test only! See BackupCacheStoppedTest
    */
   public ByteString getLocalCacheForRemoteSite(String remoteSite, ByteString remoteCache) {
      LocalSiteCache cache = localCachesMap.get(new RemoteSiteCache(remoteSite, remoteCache));
      return cache == null ? null : cache.cacheName;
   }

   private LocalSiteCache findLocalCacheForRemoteSite(String remoteSite, ByteString remoteCache) {
      RemoteSiteCache key = new RemoteSiteCache(remoteSite, remoteCache);
      return localCachesMap.computeIfAbsent(key, this::lookupLocalCaches);
   }

   private LocalSiteCache lookupLocalCaches(RemoteSiteCache remoteSiteCache) {
      for (String name : getCacheNames()) {
         Configuration configuration = getCacheConfiguration(name);
         if (configuration != null && isBackupForRemoteCache(configuration, remoteSiteCache, name)) {
            return new LocalSiteCache(ByteString.fromString(name), isLocal(configuration));
         }
      }

      String name = remoteSiteCache.originCache.toString();
      log.debugf("Did not find any backup explicitly configured backup cache for remote cache/site: %s/%s. Using %s",
            remoteSiteCache.originSite, name, name);
      Configuration configuration = getCacheConfiguration(name);
      if (configuration == null) {
         return null;
      }

      return new LocalSiteCache(remoteSiteCache.originCache, isLocal(configuration));
   }

   private Collection<String> getCacheNames() {
      return globalComponentRegistry.getCacheManager().getCacheNames();
   }

   private Configuration getCacheConfiguration(String cacheName) {
      return globalComponentRegistry.getComponent(ConfigurationManager.class).getConfiguration(cacheName, false);
   }

   private static boolean isLocal(Configuration configuration) {
      return !configuration.clustering().cacheMode().isClustered();
   }

   private boolean isBackupForRemoteCache(Configuration cacheConfiguration, RemoteSiteCache remoteSite, String localCacheName) {
      String remoteSiteName = remoteSite.originSite;
      String remoteCacheName = remoteSite.originCache.toString();
      boolean found = cacheConfiguration.sites().backupFor().isBackupFor(remoteSiteName, remoteCacheName);
      if (log.isTraceEnabled() && found) {
         log.tracef("Found local cache '%s' is backup for cache '%s' from site '%s'", localCacheName, remoteCacheName,
               remoteSiteName);
      }
      return found;
   }

   private static class ReplicableCommandRunner extends ResponseConsumer implements Runnable {

      private final GlobalComponentRegistry globalComponentRegistry;
      private final boolean preserveOrder;

      private ReplicableCommandRunner(ReplicableCommand command, Reply reply,
            GlobalComponentRegistry globalComponentRegistry, boolean preserveOrder) {
         super(command, reply);
         this.globalComponentRegistry = globalComponentRegistry;
         this.preserveOrder = preserveOrder;
      }

      @Override
      public void run() {
         try {
            CompletionStage<?> stage;
            if (command instanceof GlobalRpcCommand) {
               stage = ((GlobalRpcCommand) command).invokeAsync(globalComponentRegistry).whenComplete(this);
            } else {
               globalComponentRegistry.wireDependencies(command);
               stage = command.invokeAsync().whenComplete(this);
            }
            if (preserveOrder) {
               CompletionStages.join(stage);
            }
         } catch (Throwable throwable) {
            accept(null, throwable);
         }
      }
   }

   private static class ResponseConsumer implements BiConsumer<Object, Throwable> {

      final ReplicableCommand command;
      private final Reply reply;

      private ResponseConsumer(ReplicableCommand command, Reply reply) {
         this.command = command;
         this.reply = reply;
      }

      @Override
      public void accept(Object retVal, Throwable throwable) {
         reply.reply(convertToResponse(retVal, throwable));
      }

      private Response convertToResponse(Object retVal, Throwable throwable) {
         if (throwable != null) {
            throwable = CompletableFutures.extractException(throwable);
            if (throwable instanceof InterruptedException || throwable instanceof IllegalLifecycleStateException) {
               CLUSTER.debugf("Shutdown while handling command %s", command);
               return shuttingDownResponse();
            } else {
               CLUSTER.exceptionHandlingCommand(command, throwable);
               return exceptionHandlingCommand(throwable);
            }
         } else {
            if (retVal == null || retVal instanceof Response) {
               return (Response) retVal;
            } else {
               return SuccessfulResponse.create(retVal);
            }
         }
      }
   }

   private static class RemoteSiteCache {
      private final String originSite;
      private final ByteString originCache;

      private RemoteSiteCache(String originSite, ByteString originCache) {
         this.originSite = originSite;
         this.originCache = originCache;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) {
            return true;
         }
         if (o == null || getClass() != o.getClass()) {
            return false;
         }
         RemoteSiteCache remoteSiteCache = (RemoteSiteCache) o;
         return Objects.equals(originSite, remoteSiteCache.originSite) &&
               Objects.equals(originCache, remoteSiteCache.originCache);
      }

      @Override
      public int hashCode() {
         return Objects.hash(originSite, originCache);
      }
   }

   private static class LocalSiteCache {
      private final ByteString cacheName;
      private final boolean local;

      private LocalSiteCache(ByteString cacheName, boolean local) {
         this.cacheName = cacheName;
         this.local = local;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) {
            return true;
         }
         if (o == null || getClass() != o.getClass()) {
            return false;
         }
         LocalSiteCache that = (LocalSiteCache) o;
         return local == that.local &&
               Objects.equals(cacheName, that.cacheName);
      }

      @Override
      public int hashCode() {
         return Objects.hash(cacheName, local);
      }
   }

}
