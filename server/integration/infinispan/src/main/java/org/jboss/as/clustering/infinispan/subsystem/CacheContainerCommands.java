package org.jboss.as.clustering.infinispan.subsystem;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.infinispan.spi.service.CacheContainerServiceName;
import org.infinispan.util.logging.events.EventLog;
import org.infinispan.util.logging.events.EventLogManager;
import org.infinispan.util.logging.events.EventLogger;
import org.infinispan.xsite.GlobalXSiteAdminOperations;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.OperationStepHandler;
import org.jboss.as.controller.PathAddress;
import org.jboss.dmr.ModelNode;
import org.jboss.msc.service.ServiceController;

import static org.jboss.as.clustering.infinispan.InfinispanMessages.MESSAGES;
import static org.jboss.as.controller.PathAddress.pathAddress;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP_ADDR;

import java.util.List;

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
              throw new OperationFailedException(MESSAGES.failedToInvokeOperation(e.getLocalizedMessage()));
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
      protected ModelNode invokeCommand(EmbeddedCacheManager cacheManager, OperationContext context, ModelNode operation) throws Exception {
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
      protected ModelNode invokeCommand(EmbeddedCacheManager cacheManager, OperationContext context, ModelNode operation) throws Exception {
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
      protected ModelNode invokeCommand(EmbeddedCacheManager cacheManager, OperationContext context, ModelNode operation) throws Exception {
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
      protected ModelNode invokeCommand(EmbeddedCacheManager cacheManager, OperationContext context, ModelNode operation) throws Exception {
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
          int offset = CacheContainerResource.OFFSET.resolveModelAttribute(context, operation).asInt();
          EventLogger eventLogger = EventLogManager.getEventLogger(cacheManager);
          List<EventLog> events = eventLogger.getEvents(offset, count);
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
}
