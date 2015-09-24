package org.infinispan.tasks.impl;

import java.lang.invoke.MethodHandles;
import java.time.Instant;
import java.time.temporal.TemporalAmount;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.Search;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.remoting.transport.Address;
import org.infinispan.tasks.TaskContext;
import org.infinispan.tasks.TaskEvent;
import org.infinispan.tasks.TaskEventStatus;
import org.infinispan.tasks.TaskManager;
import org.infinispan.tasks.impl.history.TaskEventImpl;
import org.infinispan.tasks.logging.Log;
import org.infinispan.tasks.spi.TaskEngine;
import org.infinispan.util.TimeService;
import org.infinispan.util.logging.LogFactory;

@Scope(Scopes.GLOBAL)
public class TaskManagerImpl implements TaskManager {
   public static final String TASK_HISTORY_CACHE = "___task_history_cache";
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass(), Log.class);
   private EmbeddedCacheManager cacheManager;
   private Set<TaskEngine> engines;
   private Cache<UUID, TaskEvent> taskEventHistoryCache;
   private QueryFactory<?> queryFactory;
   private TimeService timeService;

   public TaskManagerImpl() {
      engines = new HashSet<>();
   }

   @Inject
   public void initialize(final EmbeddedCacheManager cacheManager, InternalCacheRegistry internalCacheRegistry, final TimeService timeService) {
      this.cacheManager = cacheManager;
      internalCacheRegistry.registerInternalCache(TASK_HISTORY_CACHE, getTaskHistoryCacheConfiguration().build(),
            EnumSet.of(InternalCacheRegistry.Flag.PERSISTENT));
      this.timeService = timeService;
   }

   private ConfigurationBuilder getTaskHistoryCacheConfiguration() {
      GlobalConfiguration globalConfiguration = cacheManager.getGlobalComponentRegistry().getGlobalConfiguration();
      CacheMode cacheMode = globalConfiguration.isClustered() ? CacheMode.REPL_SYNC : CacheMode.LOCAL;

      ConfigurationBuilder cfg = new ConfigurationBuilder();
      cfg.clustering().cacheMode(cacheMode).sync().stateTransfer().fetchInMemoryState(true).awaitInitialTransfer(false);
      cfg.eviction().maxEntries(100l);
      return cfg;
   }

   public synchronized void registerTaskEngine(TaskEngine engine) {
      if (engines.contains(engine)) {
         throw log.duplicateTaskEngineRegistration(engine.getName());
      } else {
         engines.add(engine);
      }
   }

   private Cache<UUID, TaskEvent> getTaskHistoryCache() {
      if (taskEventHistoryCache == null) {
         taskEventHistoryCache = cacheManager.getCache(TASK_HISTORY_CACHE);
         queryFactory = Search.getQueryFactory(taskEventHistoryCache);
      }
      return taskEventHistoryCache;
   }

   @Override
   public <T> CompletableFuture<T> runTask(String name, TaskContext context) {
      for(TaskEngine engine : engines) {
         if (engine.handles(name)) {
            Address address = cacheManager.getAddress();
            TaskEventImpl event = new TaskEventImpl(name, address == null ? "local" : address.toString(), context);
            event.setStart(timeService.instant());
            Cache<UUID, TaskEvent> taskHistory = getTaskHistoryCache();
            taskHistory.put(event.getUUID(), event);
            CompletableFuture<T> task = engine.runTask(name, context);
            return task.whenComplete((r, e) -> {
               if (e != null) {
                  event.setStatus(TaskEventStatus.ERROR);
                  event.setLog(Optional.of(e.getMessage()));
               } else {
                  event.setStatus(TaskEventStatus.SUCCESS);
               }
               event.setFinish(timeService.instant());
               taskHistory.put(event.getUUID(), event);
            });
         }
      }
      throw log.unknownTask(name);
   }

   @Override
   public List<TaskEvent> getTaskHistory(Instant fromInstant, Instant toInstant) {
      Query query = queryFactory.from(TaskEventImpl.class).having("start").between(fromInstant, toInstant).toBuilder().build();
      return query.list();
   }

   @Override
   public List<TaskEvent> getTaskHistory(TemporalAmount temporalAmount) {
      Instant startInstant = timeService.instant().minus(temporalAmount);
      Query query = queryFactory.from(TaskEventImpl.class).having("start").gte(startInstant).toBuilder().build();
      return query.list();
   }

   @Override
   public List<TaskEvent> getTaskHistory(String taskName, Instant fromInstant, Instant toInstant) {
      Query query = queryFactory.from(TaskEventImpl.class).having("name").eq(taskName)
            .and().having("start").between(fromInstant, toInstant).toBuilder().build();
      return query.list();
   }

   @Override
   public List<TaskEvent> getTaskHistory(String taskName, TemporalAmount temporalAmount) {
      Instant startInstant = timeService.instant().minus(temporalAmount);
      Query query = queryFactory.from(TaskEventImpl.class).having("name").eq(taskName).and().having("start").gte(startInstant).toBuilder().build();
      return query.list();
   }

   @Override
   public void clearTaskHistory() {
      getTaskHistoryCache().clear();
   }

}
