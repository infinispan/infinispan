package org.jboss.as.clustering.infinispan.subsystem;

import static org.jboss.as.clustering.infinispan.InfinispanMessages.MESSAGES;
import static org.jboss.as.controller.PathAddress.pathAddress;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP_ADDR;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.scripting.ScriptingManager;
import org.infinispan.server.infinispan.SecurityActions;
import org.infinispan.server.infinispan.spi.service.CacheContainerServiceName;
import org.infinispan.tasks.Task;
import org.infinispan.tasks.TaskContext;
import org.infinispan.tasks.TaskExecution;
import org.infinispan.tasks.TaskManager;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.util.logging.events.EventLog;
import org.infinispan.util.logging.events.EventLogCategory;
import org.infinispan.util.logging.events.EventLogLevel;
import org.infinispan.util.logging.events.EventLogManager;
import org.infinispan.util.logging.events.EventLogger;
import org.infinispan.xsite.GlobalXSiteAdminOperations;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.OperationStepHandler;
import org.jboss.as.controller.PathAddress;
import org.jboss.dmr.ModelNode;
import org.jboss.msc.service.ServiceController;

/**
 * Custom commands related to the cache container.
 *
 * @author Pedro Ruivo
 * @since 8.1
 */
public abstract class CacheContainerCommands implements OperationStepHandler {
   final protected int pathOffset;

   CacheContainerCommands(int pathOffset) {
      this.pathOffset = pathOffset;
   }

   private static ModelNode toOperationResult(String s) {
      ModelNode result = new ModelNode();
      result.add(s);
      return result;
   }

   /**
    * An attribute write handler which performs special processing for ALIAS attributes.
    *
    * @param context   the operation context
    * @param operation the operation being executed
    * @throws org.jboss.as.controller.OperationFailedException
    */
   @Override
   public void execute(OperationContext context, ModelNode operation) throws OperationFailedException {
       if (context.isNormalServer()) {
           ModelNode operationResult;
           try {
              operationResult = invokeCommand(getEmbeddedCacheManager(context, operation), context, operation);
           } catch (Exception e) {
              throw new OperationFailedException(MESSAGES.failedToInvokeOperation(e.getLocalizedMessage()), e);
           }
           if (operationResult != null) {
              context.getResult().set(operationResult);
           }
       }
   }

   protected abstract ModelNode invokeCommand(EmbeddedCacheManager cacheManager, OperationContext context, ModelNode operation) throws Exception;

   private EmbeddedCacheManager getEmbeddedCacheManager(OperationContext context, ModelNode operation) {
      final PathAddress address = pathAddress(operation.require(OP_ADDR));
      final String cacheContainerName = address.getElement(address.size() - 1 - pathOffset).getValue();
      final ServiceController<?> controller = context.getServiceRegistry(false)
            .getService(CacheContainerServiceName.CACHE_CONTAINER.getServiceName(cacheContainerName));
      return (EmbeddedCacheManager) controller.getValue();
   }

   public static class BackupTakeSiteOfflineCommand extends CacheContainerCommands {
      public static final BackupTakeSiteOfflineCommand INSTANCE = new BackupTakeSiteOfflineCommand();

      public BackupTakeSiteOfflineCommand() {
         super(0);
      }

      @Override
      protected ModelNode invokeCommand(EmbeddedCacheManager cacheManager, OperationContext context, ModelNode operation) {
         final String siteNameParameter = CacheContainerResource.SITE_NAME.getName();
         final ModelNode siteName = operation.require(siteNameParameter);
         GlobalXSiteAdminOperations xsiteAdminOperations = cacheManager.getGlobalComponentRegistry().getComponent(GlobalXSiteAdminOperations.class);
         return toOperationResult(xsiteAdminOperations.takeSiteOffline(siteName.asString()));
      }
   }

   public static class BackupBringSiteOnlineCommand extends CacheContainerCommands {
      public static final BackupBringSiteOnlineCommand INSTANCE = new BackupBringSiteOnlineCommand();

      public BackupBringSiteOnlineCommand() {
         super(0);
      }

      @Override
      protected ModelNode invokeCommand(EmbeddedCacheManager cacheManager, OperationContext context, ModelNode operation) {
         final String siteNameParameter = CacheContainerResource.SITE_NAME.getName();
         final ModelNode siteName = operation.require(siteNameParameter);
         GlobalXSiteAdminOperations xsiteAdminOperations = cacheManager.getGlobalComponentRegistry().getComponent(GlobalXSiteAdminOperations.class);
         return toOperationResult(xsiteAdminOperations.bringSiteOnline(siteName.asString()));
      }
   }

   public static class BackupPushStateCommand extends CacheContainerCommands {
      public static final BackupPushStateCommand INSTANCE = new BackupPushStateCommand();

      public BackupPushStateCommand() {
         super(0);
      }

      @Override
      protected ModelNode invokeCommand(EmbeddedCacheManager cacheManager, OperationContext context, ModelNode operation) {
         final String siteNameParameter = CacheContainerResource.SITE_NAME.getName();
         final ModelNode siteName = operation.require(siteNameParameter);
         GlobalXSiteAdminOperations xsiteAdminOperations = cacheManager.getGlobalComponentRegistry().getComponent(GlobalXSiteAdminOperations.class);
         return toOperationResult(xsiteAdminOperations.pushState(siteName.asString()));
      }
   }

   public static class BackupCancelPushStateCommand extends CacheContainerCommands {
      public static final BackupCancelPushStateCommand INSTANCE = new BackupCancelPushStateCommand();

      public BackupCancelPushStateCommand() {
         super(0);
      }

      @Override
      protected ModelNode invokeCommand(EmbeddedCacheManager cacheManager, OperationContext context, ModelNode operation) {
         final String siteNameParameter = CacheContainerResource.SITE_NAME.getName();
         final ModelNode siteName = operation.require(siteNameParameter);
         GlobalXSiteAdminOperations xsiteAdminOperations = cacheManager.getGlobalComponentRegistry().getComponent(GlobalXSiteAdminOperations.class);
         return toOperationResult(xsiteAdminOperations.cancelPushState(siteName.asString()));
      }
   }

   public static class ReadEventLogCommand extends CacheContainerCommands {
       public static final ReadEventLogCommand INSTANCE = new ReadEventLogCommand();

       public ReadEventLogCommand() {
          super(0);
       }

       @Override
       protected ModelNode invokeCommand(EmbeddedCacheManager cacheManager, OperationContext context, ModelNode operation) throws Exception {
          int count = CacheContainerResource.COUNT.resolveModelAttribute(context, operation).asInt();
          ModelNode sinceNode = CacheContainerResource.SINCE.resolveModelAttribute(context, operation);
          Instant since = sinceNode.isDefined() ? ZonedDateTime.parse(sinceNode.asString(), DateTimeFormatter.ISO_DATE_TIME).toInstant() : Instant.now();
          ModelNode categoryNode = CacheContainerResource.CATEGORY.resolveModelAttribute(context, operation);
          Optional<EventLogCategory> category = categoryNode.isDefined() ? Optional.of(EventLogCategory.valueOf(categoryNode.asString())) : Optional.empty();
          ModelNode levelNode = CacheContainerResource.LEVEL.resolveModelAttribute(context, operation);
          Optional<EventLogLevel> level = levelNode.isDefined() ? Optional.of(EventLogLevel.valueOf(levelNode.asString())) : Optional.empty();
          EventLogger eventLogger = EventLogManager.getEventLogger(cacheManager);
          List<EventLog> events = eventLogger.getEvents(since, count, category, level);
          final ModelNode result = new ModelNode().setEmptyList();
          for (EventLog event : events) {
              ModelNode node = result.addEmptyObject();
              node.get("when").set(event.getWhen().toString());
              node.get("level").set(event.getLevel().toString());
              node.get("category").set(event.getCategory().toString());
              node.get("message").set(event.getMessage());
              event.getDetail().ifPresent(detail -> node.get("detail").set(detail));
              event.getContext().ifPresent(ctx -> node.get("context").set(ctx));
              event.getScope().ifPresent(scope -> node.get("scope").set(scope));
              event.getWho().ifPresent(who -> node.get("who").set(who));
          }
          return result;
       }
    }

    public static class TaskListCommand extends CacheContainerCommands {
        public static final TaskListCommand INSTANCE = new TaskListCommand();

        public TaskListCommand() {
            super(0);
        }

        @Override
        protected ModelNode invokeCommand(EmbeddedCacheManager cacheManager, OperationContext context, ModelNode operation) {
            TaskManager taskManager = cacheManager.getGlobalComponentRegistry().getComponent(TaskManager.class);
            List<Task> tasks = taskManager.getTasks();
            tasks.sort(Comparator.comparing(Task::getName));
            final ModelNode result = new ModelNode().setEmptyList();
            for (Task task : tasks) {
                ModelNode node = result.addEmptyObject();
                node.get("name").set(task.getName());
                node.get("type").set(task.getType());
                node.get("mode").set(task.getExecutionMode().toString());
                ModelNode parameters = node.get("parameters").setEmptyList();
                task.getParameters().forEach(p -> parameters.add(new ModelNode().set(p)));
            }
            return result;
        }
    }

    public static class TaskExecuteCommand extends CacheContainerCommands {
        public static final TaskExecuteCommand INSTANCE = new TaskExecuteCommand();

        public TaskExecuteCommand() {
            super(0);
        }

        @Override
        protected ModelNode invokeCommand(EmbeddedCacheManager cacheManager, OperationContext context, ModelNode operation) throws Exception {
            String taskName = CacheContainerResource.TASK_NAME.resolveModelAttribute(context, operation).asString();
            boolean taskAsync = CacheContainerResource.TASK_ASYNC.resolveModelAttribute(context, operation).asBoolean();
            TaskManager taskManager = cacheManager.getGlobalComponentRegistry().getComponent(TaskManager.class);
            TaskContext taskContext = new TaskContext();
            ModelNode cacheNameNode = CacheContainerResource.TASK_CACHE_NAME.resolveModelAttribute(context, operation);
            if (cacheNameNode.isDefined()) {
                taskContext.cache(cacheManager.getCache(cacheNameNode.asString(), false));
            }
            ModelNode parameters = CacheContainerResource.TASK_PARAMETERS.resolveModelAttribute(context, operation);
            if (parameters.isDefined()) {
                parameters.asPropertyList().forEach(property -> taskContext.addParameter(property.getName(), property.getValue().asString()));
            }
            taskContext.logEvent(true);
            CompletableFuture<Object> taskFuture = taskManager.runTask(taskName, taskContext);
            if(taskAsync) {
                return new ModelNode();
            } else {
                Object result = taskFuture.get();
                return new ModelNode(String.valueOf(result));
            }
        }
    }

    public static class TaskStatusCommand extends CacheContainerCommands {
        public static final TaskStatusCommand INSTANCE = new TaskStatusCommand();

        public TaskStatusCommand() {
            super(0);
        }

        @Override
        protected ModelNode invokeCommand(EmbeddedCacheManager cacheManager, OperationContext context, ModelNode operation) {
            TaskManager taskManager = cacheManager.getGlobalComponentRegistry().getComponent(TaskManager.class);
            List<TaskExecution> taskExecutions = taskManager.getCurrentTasks();
            taskExecutions.sort(Comparator.comparing(TaskExecution::getStart));
            final ModelNode result = new ModelNode().setEmptyList();
            for (TaskExecution execution : taskExecutions) {
                ModelNode node = result.addEmptyObject();
                node.get("name").set(execution.getName());
                node.get("start").set(execution.getStart().toString());
                node.get("where").set(execution.getWhere());
                execution.getWhat().ifPresent(what -> node.get("context").set(what));
                execution.getWho().ifPresent(who -> node.get("who").set(who));
            }
            return result;
        }
    }

    public static class ScriptAddCommand extends CacheContainerCommands {
        public static final ScriptAddCommand INSTANCE = new ScriptAddCommand();

        public ScriptAddCommand() {
            super(0);
        }

        @Override
        protected ModelNode invokeCommand(EmbeddedCacheManager cacheManager, OperationContext context, ModelNode operation) throws Exception {
            ScriptingManager scriptManager = cacheManager.getGlobalComponentRegistry().getComponent(ScriptingManager.class);
            String scriptName = CacheContainerResource.SCRIPT_NAME.resolveModelAttribute(context, operation).asString();
            String scriptCode = CacheContainerResource.SCRIPT_CODE.resolveModelAttribute(context, operation).asString();
            scriptManager.addScript(scriptName, scriptCode);
            return null;
        }
    }

    public static class ScriptCatCommand extends CacheContainerCommands {
        public static final ScriptCatCommand INSTANCE = new ScriptCatCommand();

        public ScriptCatCommand() {
            super(0);
        }

        @Override
        protected ModelNode invokeCommand(EmbeddedCacheManager cacheManager, OperationContext context, ModelNode operation) throws Exception {
            ScriptingManager scriptManager = cacheManager.getGlobalComponentRegistry().getComponent(ScriptingManager.class);
            String scriptName = CacheContainerResource.SCRIPT_NAME.resolveModelAttribute(context, operation).asString();
            String scriptCode = scriptManager.getScript(scriptName);
            return scriptCode != null ? new ModelNode().set(scriptCode) : null;
        }
    }

    public static class ScriptRemoveCommand extends CacheContainerCommands {
        public static final ScriptRemoveCommand INSTANCE = new ScriptRemoveCommand();

        public ScriptRemoveCommand() {
            super(0);
        }

        @Override
        protected ModelNode invokeCommand(EmbeddedCacheManager cacheManager, OperationContext context, ModelNode operation) throws Exception {
            ScriptingManager scriptManager = cacheManager.getGlobalComponentRegistry().getComponent(ScriptingManager.class);
            String scriptName = CacheContainerResource.SCRIPT_NAME.resolveModelAttribute(context, operation).asString();
            scriptManager.removeScript(scriptName);
            return null;
        }
    }

   public static class ClusterRebalanceCommand extends CacheContainerCommands {

      public static final ClusterRebalanceCommand INSTANCE = new ClusterRebalanceCommand();

      private ClusterRebalanceCommand() {
         super(0);
      }

      @Override
      protected ModelNode invokeCommand(EmbeddedCacheManager cacheManager, OperationContext context, ModelNode operation) throws Exception {
         boolean value = CacheContainerResource.BOOL_VALUE.resolveModelAttribute(context, operation).asBoolean();
         LocalTopologyManager topologyManager = SecurityActions.getGlobalComponentRegistry(cacheManager)
               .getComponent(LocalTopologyManager.class);
         if (topologyManager != null) {
            topologyManager.setRebalancingEnabled(value);
         }
         return null;
      }
   }
}
