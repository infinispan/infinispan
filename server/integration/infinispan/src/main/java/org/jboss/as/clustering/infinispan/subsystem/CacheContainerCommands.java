package org.jboss.as.clustering.infinispan.subsystem;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.infinispan.spi.service.CacheContainerServiceName;
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
      ModelNode operationResult;
      try {
         operationResult = invokeCommand(getEmbeddedCacheManager(context, operation), operation);
      } catch (Exception e) {
         throw new OperationFailedException(new ModelNode().set(MESSAGES.failedToInvokeOperation(e.getLocalizedMessage())));
      }
      if (operationResult != null) {
         context.getResult().set(operationResult);
      }
      context.stepCompleted();
   }

   protected abstract ModelNode invokeCommand(EmbeddedCacheManager cacheManager, ModelNode operation) throws Exception;

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
      protected ModelNode invokeCommand(EmbeddedCacheManager cacheManager, ModelNode operation) throws Exception {
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
      protected ModelNode invokeCommand(EmbeddedCacheManager cacheManager, ModelNode operation) throws Exception {
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
      protected ModelNode invokeCommand(EmbeddedCacheManager cacheManager, ModelNode operation) throws Exception {
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
      protected ModelNode invokeCommand(EmbeddedCacheManager cacheManager, ModelNode operation) throws Exception {
         final String siteNameParameter = CacheContainerResource.SITE_NAME.getName();
         final ModelNode siteName = operation.require(siteNameParameter);
         GlobalXSiteAdminOperations xsiteAdminOperations = cacheManager.getGlobalComponentRegistry().getComponent(GlobalXSiteAdminOperations.class);
         return toOperationResult(xsiteAdminOperations.cancelPushState(siteName.asString()));
      }
   }
}
