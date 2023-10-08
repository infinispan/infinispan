package org.infinispan.tasks.impl;

import static org.infinispan.tasks.logging.Messages.MESSAGES;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import javax.security.auth.Subject;

import org.infinispan.commons.time.TimeService;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.security.Security;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.tasks.Task;
import org.infinispan.tasks.TaskContext;
import org.infinispan.tasks.TaskExecution;
import org.infinispan.tasks.TaskManager;
import org.infinispan.tasks.logging.Log;
import org.infinispan.tasks.spi.NonBlockingTaskEngine;
import org.infinispan.tasks.spi.TaskEngine;
import org.infinispan.util.concurrent.BlockingManager;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.util.logging.events.EventLogCategory;
import org.infinispan.util.logging.events.EventLogManager;
import org.infinispan.util.logging.events.EventLogger;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Maybe;

/**
 * TaskManagerImpl.
 *
 * @author Tristan Tarrant
 * @since 8.1
 */
@Scope(Scopes.GLOBAL)
public class TaskManagerImpl implements TaskManager {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass(), Log.class);

   @Inject EmbeddedCacheManager cacheManager;
   @Inject TimeService timeService;
   @Inject
   BlockingManager blockingManager;
   @Inject EventLogManager eventLogManager;

   private final List<TaskEngine> engines;
   private final ConcurrentMap<UUID, TaskExecution> runningTasks;
   private boolean useSecurity;

   public TaskManagerImpl() {
      engines = new ArrayList<>();
      runningTasks = new ConcurrentHashMap<>();
   }

   @Start
   public void start() {
      this.useSecurity = SecurityActions.getCacheManagerConfiguration(cacheManager).security().authorization().enabled();
   }

   @Override
   public synchronized void registerTaskEngine(TaskEngine engine) {
      if (!engines.contains(engine)) {
         engines.add(engine);
      }
   }

   @Override
   public <T> CompletionStage<T> runTask(String name, TaskContext context) {
      // This finds an engine that can accept the task
      CompletionStage<TaskEngine> engineStage = Flowable.fromIterable(engines)
            .concatMapMaybe(engine -> {
               if (engine instanceof NonBlockingTaskEngine) {
                  return Maybe.fromCompletionStage(((NonBlockingTaskEngine) engine).handlesAsync(name))
                        .concatMap(canHandle -> canHandle ? Maybe.just(engine) : Maybe.empty());
               }
               return engine.handles(name) ? Maybe.just(engine) : Maybe.empty();
            })
            .firstElement()
            .toCompletionStage(null);

      // Performs the actual task if an engine was found
      return engineStage.thenCompose(engine -> {
         if (engine == null) {
            throw log.unknownTask(name);
         }
         context.cacheManager(cacheManager);
         Address address = cacheManager.getAddress();
         Subject subject = context.getSubject().orElseGet(() -> {
            if(useSecurity) {
               return Security.getSubject();
            } else {
               return null;
            }
         });
         String who = subject == null ? null : Security.getSubjectUserPrincipal(subject).getName();
         TaskExecutionImpl exec = new TaskExecutionImpl(name, address == null ? "local" : address.toString(), who, context);
         exec.setStart(timeService.instant());
         runningTasks.put(exec.getUUID(), exec);
         CompletionStage<T> task = engine.runTask(name, context, blockingManager);
         return task.whenComplete((r, e) -> {
            if (context.isLogEvent()) {
               EventLogger eventLog = eventLogManager.getEventLogger().scope(cacheManager.getAddress());
               if (who != null)
                  eventLog.who(who);

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
      });
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

   @Override
   public CompletionStage<List<Task>> getTasksAsync() {
      return taskFlowable()
            .collect(Collectors.toList())
            .toCompletionStage();
   }

   private Flowable<Task> taskFlowable() {
      return Flowable.fromIterable(engines)
            .flatMap(engine -> {
               if (engine instanceof NonBlockingTaskEngine) {
                  return Flowable.fromCompletionStage(((NonBlockingTaskEngine) engine).getTasksAsync())
                        .flatMap(Flowable::fromIterable);
               }
               return Flowable.fromIterable(engine.getTasks());
            });
   }

   @Override
   public List<Task> getUserTasks() {
      return engines.stream().flatMap(engine -> engine.getTasks().stream())
            .filter(t -> !t.getName().startsWith("@@"))
            .collect(Collectors.toList());
   }

   @Override
   public CompletionStage<List<Task>> getUserTasksAsync() {
      return taskFlowable()
            .filter(t -> !t.getName().startsWith("@@"))
            .collect(Collectors.toList())
            .toCompletionStage();
   }
}
