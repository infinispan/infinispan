package org.infinispan.tasks.impl;

import static org.infinispan.tasks.logging.Messages.MESSAGES;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;

import javax.security.auth.Subject;

import org.infinispan.commons.time.TimeService;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.security.Security;
import org.infinispan.tasks.Task;
import org.infinispan.tasks.TaskContext;
import org.infinispan.tasks.TaskExecution;
import org.infinispan.tasks.TaskManager;
import org.infinispan.tasks.logging.Log;
import org.infinispan.tasks.spi.TaskEngine;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.util.logging.events.EventLogCategory;
import org.infinispan.util.logging.events.EventLogManager;
import org.infinispan.util.logging.events.EventLogger;

/**
 * TaskManagerImpl.
 *
 * @author Tristan Tarrant
 * @since 8.1
 */
@Scope(Scopes.GLOBAL)
public class TaskManagerImpl implements TaskManager {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass(), Log.class);

   @Inject private EmbeddedCacheManager cacheManager;
   @Inject private TimeService timeService;
   @Inject @ComponentName(KnownComponentNames.ASYNC_OPERATIONS_EXECUTOR)
   private ExecutorService asyncExecutor;

   private List<TaskEngine> engines;
   private ConcurrentMap<UUID, TaskExecution> runningTasks;
   private boolean useSecurity;

   public TaskManagerImpl() {
      engines = new ArrayList<>();
      runningTasks = new ConcurrentHashMap<>();
   }

   @Start
   public void start() {
      this.useSecurity = cacheManager.getCacheManagerConfiguration().security().authorization().enabled();
   }

   @Override
   public synchronized void registerTaskEngine(TaskEngine engine) {
      if (!engines.contains(engine)) {
         engines.add(engine);
      }
   }

   @Override
   public <T> CompletableFuture<T> runTask(String name, TaskContext context) {
      for(TaskEngine engine : engines) {
         if (engine.handles(name)) {
            context.cacheManager(cacheManager);
            Address address = cacheManager.getAddress();
            Subject subject = context.getSubject().orElseGet(() -> {
               if(useSecurity) {
                  return Security.getSubject();
               } else {
                  return null;
               }
            });
            Optional<String> who = Optional.ofNullable(subject == null ? null : Security.getSubjectUserPrincipal(subject).getName());
            TaskExecutionImpl exec = new TaskExecutionImpl(name, address == null ? "local" : address.toString(), who, context);
            exec.setStart(timeService.instant());
            runningTasks.put(exec.getUUID(), exec);
            CompletableFuture<T> task = engine.runTask(name, context, asyncExecutor);
            return task.whenComplete((r, e) -> {
               if (context.isLogEvent()) {
                  EventLogger eventLog = EventLogManager.getEventLogger(cacheManager).scope(cacheManager.getAddress());
                  who.ifPresent(eventLog::who);
                  context.getCache().ifPresent(eventLog::context);
                  if (e != null) {
                     eventLog.detail(e)
                           .error(EventLogCategory.TASKS, MESSAGES.taskFailure(name));
                  } else {
                     eventLog.detail(String.valueOf(r))
                           .info(EventLogCategory.TASKS, MESSAGES.taskSuccess(name));
                  }
               }
               runningTasks.remove(exec.getUUID());
            });
         }
      }
      throw log.unknownTask(name);
   }

   @Override
   public List<TaskExecution> getCurrentTasks() {
      return new ArrayList<>(runningTasks.values());
   }

   @Override
   public List<TaskEngine> getEngines() {
      return Collections.unmodifiableList(engines);
   }

   @Override
   public List<Task> getTasks() {
      List<Task> tasks = new ArrayList<>();
      engines.forEach(engine -> tasks.addAll(engine.getTasks()));
      return tasks;
   }

}
