package org.jboss.as.clustering.infinispan.subsystem;

import org.infinispan.Cache;
import org.infinispan.eviction.PassivationManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.interceptors.ActivationInterceptor;
import org.infinispan.interceptors.CacheMgmtInterceptor;
import org.infinispan.interceptors.CacheWriterInterceptor;
import org.infinispan.interceptors.InvalidationInterceptor;
import org.infinispan.interceptors.TxInterceptor;
import org.infinispan.query.SearchManager;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcManagerImpl;
import org.infinispan.server.infinispan.SecurityActions;
import org.infinispan.transaction.xa.recovery.RecoveryAdminOperations;
import org.infinispan.upgrade.RollingUpgradeManager;
import org.infinispan.xsite.XSiteAdminOperations;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.OperationStepHandler;
import org.jboss.as.controller.PathAddress;
import org.jboss.dmr.ModelNode;
import org.jboss.msc.service.ServiceController;

import java.util.Iterator;
import java.util.Map;

import static org.jboss.as.clustering.infinispan.InfinispanMessages.MESSAGES;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP_ADDR;

/**
 * Custom commands related to a cache
 *
 * @author Tristan Tarrant
 */
public abstract class CacheCommands implements OperationStepHandler {

    final int pathOffset;

    CacheCommands(int pathOffset) {
        this.pathOffset = pathOffset;
    }

    /**
     * An attribute write handler which performs special processing for ALIAS attributes.
     *
     * @param context the operation context
     * @param operation the operation being executed
     * @throws org.jboss.as.controller.OperationFailedException
     */
    @Override
    public void execute(OperationContext context, ModelNode operation) throws OperationFailedException {
        final PathAddress address = PathAddress.pathAddress(operation.require(OP_ADDR));
        final String cacheContainerName = address.getElement(address.size() - 2 - pathOffset).getValue();
        final String cacheName = address.getElement(address.size() - 1 - pathOffset).getValue();
        final ServiceController<?> controller = context.getServiceRegistry(false).getService(CacheService.getServiceName(cacheContainerName, cacheName));
        Cache<?, ?> cache = (Cache<?, ?>) controller.getValue();

        ModelNode operationResult = null;
        try {
            operationResult = invokeCommand(cache, operation);
        } catch (Exception e) {
            throw new OperationFailedException(new ModelNode().set(MESSAGES.failedToInvokeOperation(e.getLocalizedMessage())));
        }
        if (operationResult != null) {
            context.getResult().set(operationResult);
        }
        context.stepCompleted();
    }

    protected abstract ModelNode invokeCommand(Cache<?, ?> cache, ModelNode operation) throws Exception;

    public static class ResetCacheStatisticsCommand extends CacheCommands {
        public static final ResetCacheStatisticsCommand INSTANCE = new ResetCacheStatisticsCommand();

        public ResetCacheStatisticsCommand() {
            super(0);
        }

        @Override
        protected ModelNode invokeCommand(Cache<?, ?> cache, ModelNode operation) throws Exception {
            SecurityActions.resetStatistics(cache.getAdvancedCache(), CacheMgmtInterceptor.class);
            return null;
        }
    }

    public static class ClearCacheCommand extends CacheCommands {
        public static final ClearCacheCommand INSTANCE = new ClearCacheCommand();

        public ClearCacheCommand() {
            super(0);
        }

        @Override
        protected ModelNode invokeCommand(Cache<?, ?> cache, ModelNode operation) throws Exception {
            SecurityActions.clearCache(cache.getAdvancedCache());
            return null;
        }
    }

    public static class StartCacheCommand extends CacheCommands {
        public static final StartCacheCommand INSTANCE = new StartCacheCommand();

        public StartCacheCommand() {
            super(0);
        }

        @Override
        protected ModelNode invokeCommand(Cache<?, ?> cache, ModelNode operation) throws Exception {
            SecurityActions.startCache(cache.getAdvancedCache());
            return null;
        }
    }

    public static class StopCacheCommand extends CacheCommands {
        public static final StopCacheCommand INSTANCE = new StopCacheCommand();

        public StopCacheCommand() {
            super(0);
        }

        @Override
        protected ModelNode invokeCommand(Cache<?, ?> cache, ModelNode operation) throws Exception {
            SecurityActions.stopCache(cache.getAdvancedCache());
            return null;
        }
    }

    public static class ResetTxStatisticsCommand extends CacheCommands {
        public static final ResetTxStatisticsCommand INSTANCE = new ResetTxStatisticsCommand();

        public ResetTxStatisticsCommand() {
            super(0);
        }

        @Override
        protected ModelNode invokeCommand(Cache<?, ?> cache, ModelNode operation) throws Exception {
            SecurityActions.resetStatistics(cache.getAdvancedCache(), TxInterceptor.class);
            return null;
        }
    }

    public static class ResetInvalidationStatisticsCommand extends CacheCommands {
        public static final ResetInvalidationStatisticsCommand INSTANCE = new ResetInvalidationStatisticsCommand();

        public ResetInvalidationStatisticsCommand() {
            super(0);
        }

        @Override
        protected ModelNode invokeCommand(Cache<?, ?> cache, ModelNode operation) throws Exception {
            SecurityActions.resetStatistics(cache.getAdvancedCache(), InvalidationInterceptor.class);
            return null;
        }
    }

    public static class ResetActivationStatisticsCommand extends CacheCommands {
        public static final ResetActivationStatisticsCommand INSTANCE = new ResetActivationStatisticsCommand();

        public ResetActivationStatisticsCommand() {
            super(0);
        }

        @Override
        protected ModelNode invokeCommand(Cache<?, ?> cache, ModelNode operation) throws Exception {
            SecurityActions.resetStatistics(cache.getAdvancedCache(), ActivationInterceptor.class);
            return null;
        }
    }

    public static class ResetPassivationStatisticsCommand extends CacheCommands {
        public static final ResetPassivationStatisticsCommand INSTANCE = new ResetPassivationStatisticsCommand();

        public ResetPassivationStatisticsCommand() {
            super(0);
        }

        @Override
        protected ModelNode invokeCommand(Cache<?, ?> cache, ModelNode operation) throws Exception {
            SecurityActions.resetStatistics(cache.getAdvancedCache(), PassivationManager.class);
            return null;
        }
    }

    public static class ResetRpcManagerStatisticsCommand extends CacheCommands {
        public static final ResetRpcManagerStatisticsCommand INSTANCE = new ResetRpcManagerStatisticsCommand();

        public ResetRpcManagerStatisticsCommand() {
            super(0);
        }

        @Override
        protected ModelNode invokeCommand(Cache<?, ?> cache, ModelNode operation) throws Exception {
            RpcManagerImpl rpcManager = (RpcManagerImpl) SecurityActions.getComponentRegistry(cache.getAdvancedCache()).getComponent(RpcManager.class);
            if (rpcManager != null) {
                rpcManager.resetStatistics();
            }
            return null;
        }
    }

    public static class ResetCacheLoaderStatisticsCommand extends CacheCommands {
        public static final ResetCacheLoaderStatisticsCommand INSTANCE = new ResetCacheLoaderStatisticsCommand();

        public ResetCacheLoaderStatisticsCommand() {
            super(0);
        }

        @Override
        protected ModelNode invokeCommand(Cache<?, ?> cache, ModelNode operation) throws Exception {
            SecurityActions.resetStatistics(cache.getAdvancedCache(), CacheWriterInterceptor.class);
            return null;
        }
    }

    public static class TransactionListInDoubtCommand extends CacheCommands {
        public static final TransactionListInDoubtCommand INSTANCE = new TransactionListInDoubtCommand();

        public TransactionListInDoubtCommand() {
            super(1);
        }

        @Override
        protected ModelNode invokeCommand(Cache<?, ?> cache, ModelNode operation) throws Exception {
            RecoveryAdminOperations recoveryAdminOperations = SecurityActions.getComponentRegistry(cache.getAdvancedCache()).getComponent(RecoveryAdminOperations.class);
            return toOperationResult(recoveryAdminOperations.showInDoubtTransactions());
        }
    }

    public static class TransactionForceCommitCommand extends CacheCommands {
        public static final TransactionForceCommitCommand INSTANCE = new TransactionForceCommitCommand();

        public TransactionForceCommitCommand() {
            super(1);
        }

        @Override
        protected ModelNode invokeCommand(Cache<?, ?> cache, ModelNode operation) throws Exception {
            long internalId = operation.require(ModelKeys.TX_INTERNAL_ID).asLong();
            RecoveryAdminOperations recoveryAdminOperations = SecurityActions.getComponentRegistry(cache.getAdvancedCache()).getComponent(RecoveryAdminOperations.class);
            return toOperationResult(recoveryAdminOperations.forceCommit(internalId));
        }
    }

    public static class TransactionForceRollbackCommand extends CacheCommands {
        public static final TransactionForceRollbackCommand INSTANCE = new TransactionForceRollbackCommand();

        public TransactionForceRollbackCommand() {
            super(1);
        }

        @Override
        protected ModelNode invokeCommand(Cache<?, ?> cache, ModelNode operation) throws Exception {
            long internalId = operation.require(ModelKeys.TX_INTERNAL_ID).asLong();
            RecoveryAdminOperations recoveryAdminOperations = SecurityActions.getComponentRegistry(cache.getAdvancedCache()).getComponent(RecoveryAdminOperations.class);
            return toOperationResult(recoveryAdminOperations.forceRollback(internalId));
        }
    }

    public static class TransactionForgetCommand extends CacheCommands {
        public static final TransactionForgetCommand INSTANCE = new TransactionForgetCommand();

        public TransactionForgetCommand() {
            super(1);
        }

        @Override
        protected ModelNode invokeCommand(Cache<?, ?> cache, ModelNode operation) throws Exception {
            long internalId = operation.require(ModelKeys.TX_INTERNAL_ID).asLong();
            RecoveryAdminOperations recoveryAdminOperations = SecurityActions.getComponentRegistry(cache.getAdvancedCache()).getComponent(RecoveryAdminOperations.class);
            return toOperationResult(recoveryAdminOperations.forget(internalId));
        }
    }

    public static class BackupBringSiteOnlineCommand extends CacheCommands {
        public static final BackupBringSiteOnlineCommand INSTANCE = new BackupBringSiteOnlineCommand();

        public BackupBringSiteOnlineCommand() {
            super(1);
        }

        @Override
        protected ModelNode invokeCommand(Cache<?, ?> cache, ModelNode operation) throws Exception {
            final PathAddress address = PathAddress.pathAddress(operation.require(OP_ADDR));
            final String site = address.getLastElement().getValue();
            XSiteAdminOperations xsiteAdminOperations = SecurityActions.getComponentRegistry(cache.getAdvancedCache()).getComponent(XSiteAdminOperations.class);
            return toOperationResult(xsiteAdminOperations.bringSiteOnline(site));
        }
    }

    public static class BackupTakeSiteOfflineCommand extends CacheCommands {
        public static final BackupTakeSiteOfflineCommand INSTANCE = new BackupTakeSiteOfflineCommand();

        public BackupTakeSiteOfflineCommand() {
            super(1);
        }


        @Override
        protected ModelNode invokeCommand(Cache<?, ?> cache, ModelNode operation) throws Exception {
            final PathAddress address = PathAddress.pathAddress(operation.require(OP_ADDR));
            final String site = address.getLastElement().getValue();
            XSiteAdminOperations xsiteAdminOperations = SecurityActions.getComponentRegistry(cache.getAdvancedCache()).getComponent(XSiteAdminOperations.class);
            return toOperationResult(xsiteAdminOperations.takeSiteOffline(site));
        }
    }

    public static class BackupSiteStatusCommand extends CacheCommands {
        public static final BackupSiteStatusCommand INSTANCE = new BackupSiteStatusCommand();

        public BackupSiteStatusCommand() {
            super(1);
        }

        @Override
        protected ModelNode invokeCommand(Cache<?, ?> cache, ModelNode operation) throws Exception {
            final PathAddress address = PathAddress.pathAddress(operation.require(OP_ADDR));
            final String site = address.getLastElement().getValue();
            XSiteAdminOperations xsiteAdminOperations = SecurityActions.getComponentRegistry(cache.getAdvancedCache()).getComponent(XSiteAdminOperations.class);
            return toOperationResult(xsiteAdminOperations.siteStatus(site));
        }
    }

    public static class SynchronizeDataCommand extends CacheCommands {
        public static final SynchronizeDataCommand INSTANCE = new SynchronizeDataCommand();

        public SynchronizeDataCommand() {
            super(0);
        }

        @Override
        protected ModelNode invokeCommand(Cache<?, ?> cache, ModelNode operation) throws Exception {
            ComponentRegistry registry = SecurityActions.getComponentRegistry(cache.getAdvancedCache());
            RollingUpgradeManager manager = registry.getComponent(RollingUpgradeManager.class);
            if (manager != null) {
                manager.synchronizeData(operation.require(ModelKeys.MIGRATOR_NAME).asString());
            }
            return null;
        }
    }

    public static class RecordGlobalKeySetCommand extends CacheCommands {
        public static final RecordGlobalKeySetCommand INSTANCE = new RecordGlobalKeySetCommand();

        public RecordGlobalKeySetCommand() {
            super(0);
        }

        @Override
        protected ModelNode invokeCommand(Cache<?, ?> cache, ModelNode operation) throws Exception {
            ComponentRegistry registry = SecurityActions.getComponentRegistry(cache.getAdvancedCache());
            RollingUpgradeManager manager = registry.getComponent(RollingUpgradeManager.class);
            if (manager != null) {
                manager.recordKnownGlobalKeyset();
            }
            return null;
        }
    }

    public static class DisconnectSourceCommand extends CacheCommands {
        public static final DisconnectSourceCommand INSTANCE = new DisconnectSourceCommand();

        public DisconnectSourceCommand() {
            super(0);
        }

        @Override
        protected ModelNode invokeCommand(Cache<?, ?> cache, ModelNode operation) throws Exception {
            ComponentRegistry registry = SecurityActions.getComponentRegistry(cache.getAdvancedCache());
            RollingUpgradeManager manager = registry.getComponent(RollingUpgradeManager.class);
            if (manager != null) {
                manager.disconnectSource(operation.require(ModelKeys.MIGRATOR_NAME).asString());
            }
            return null;
        }
   }

    public static class BackupPushStateCommand extends CacheCommands {
        public static final BackupPushStateCommand INSTANCE = new BackupPushStateCommand();

        public BackupPushStateCommand() {
         super(1);
      }

        @Override
        protected ModelNode invokeCommand(Cache<?, ?> cache, ModelNode operation) throws Exception {
            final PathAddress address = PathAddress.pathAddress(operation.require(OP_ADDR));
            final String site = address.getLastElement().getValue();
            XSiteAdminOperations xsiteAdminOperations = SecurityActions.getComponentRegistry(cache.getAdvancedCache()).getComponent(XSiteAdminOperations.class);
            return toOperationResult(xsiteAdminOperations.pushState(site));
        }
    }

    public static class BackupCancelPushStateCommand extends CacheCommands {
        public static final BackupCancelPushStateCommand INSTANCE = new BackupCancelPushStateCommand();

        public BackupCancelPushStateCommand() {
         super(1);
      }

        @Override
        protected ModelNode invokeCommand(Cache<?, ?> cache, ModelNode operation) throws Exception {
            final PathAddress address = PathAddress.pathAddress(operation.require(OP_ADDR));
            final String site = address.getLastElement().getValue();
            XSiteAdminOperations xsiteAdminOperations = SecurityActions.getComponentRegistry(cache.getAdvancedCache()).getComponent(XSiteAdminOperations.class);
            return toOperationResult(xsiteAdminOperations.cancelPushState(site));
        }
    }

    public static class BackupCancelReceiveStateCommand extends CacheCommands {
        public static final BackupCancelReceiveStateCommand INSTANCE = new BackupCancelReceiveStateCommand();

        public BackupCancelReceiveStateCommand() {
         super(1);
      }

        @Override
        protected ModelNode invokeCommand(Cache<?, ?> cache, ModelNode operation) throws Exception {
            final PathAddress address = PathAddress.pathAddress(operation.require(OP_ADDR));
            final String site = address.getLastElement().getValue();
            XSiteAdminOperations xsiteAdminOperations = SecurityActions.getComponentRegistry(cache.getAdvancedCache()).getComponent(XSiteAdminOperations.class);
            return toOperationResult(xsiteAdminOperations.cancelReceiveState(site));
        }
    }

    public static class BackupPushStateStatusCommand extends CacheCommands {
        public static final BackupPushStateStatusCommand INSTANCE = new BackupPushStateStatusCommand();

        public BackupPushStateStatusCommand() {
         super(1);
      }

        @Override
        protected ModelNode invokeCommand(Cache<?, ?> cache, ModelNode operation) throws Exception {
            XSiteAdminOperations xsiteAdminOperations = SecurityActions.getComponentRegistry(cache.getAdvancedCache()).getComponent(XSiteAdminOperations.class);
            return toOperationResult(prettyPrintMap(xsiteAdminOperations.getPushStateStatus()));
        }
    }

    public static class BackupGetSendingSiteCommand extends CacheCommands {
        public static final BackupGetSendingSiteCommand INSTANCE = new BackupGetSendingSiteCommand();

        public BackupGetSendingSiteCommand() {
         super(1);
      }

        @Override
        protected ModelNode invokeCommand(Cache<?, ?> cache, ModelNode operation) throws Exception {
            XSiteAdminOperations xsiteAdminOperations = SecurityActions.getComponentRegistry(cache.getAdvancedCache()).getComponent(XSiteAdminOperations.class);
            return toOperationResult(xsiteAdminOperations.getSendingSiteName());
        }
    }

    public static class BackupClearPushStatusCommand extends CacheCommands {
        public static final BackupClearPushStatusCommand INSTANCE = new BackupClearPushStatusCommand();

        public BackupClearPushStatusCommand() {
         super(1);
      }

        @Override
        protected ModelNode invokeCommand(Cache<?, ?> cache, ModelNode operation) throws Exception {
            XSiteAdminOperations xsiteAdminOperations = SecurityActions.getComponentRegistry(cache.getAdvancedCache()).getComponent(XSiteAdminOperations.class);
            xsiteAdminOperations.clearPushStateStatus();
            return null;
        }
    }

    public static class MassReindexCommand extends CacheCommands {
       public static final MassReindexCommand INSTANCE = new MassReindexCommand();

       public MassReindexCommand() {
           super(0);
       }

       @Override
       protected ModelNode invokeCommand(Cache<?, ?> cache, ModelNode operation) throws Exception {
           SearchManager searchManager = SecurityActions.getSearchManager(cache.getAdvancedCache());
           if (searchManager != null) {
               searchManager.getMassIndexer().start();
           }
           return null;
       }
   }

    private static ModelNode toOperationResult(String s) {
        ModelNode result = new ModelNode();
        result.add(s);
        return result;
    }

    private static String prettyPrintMap(Map<?, ?> map) {
        if (map.isEmpty()) {
           return "";
        }
        StringBuilder builder = new StringBuilder();
        for (Iterator<? extends Map.Entry<?, ?>> iterator = map.entrySet().iterator(); iterator.hasNext(); ) {
           Map.Entry<?, ?> entry = iterator.next();
           builder.append(entry.getKey()).append("=").append(entry.getValue());
           if (iterator.hasNext()) {
              builder.append(System.lineSeparator());
           }
        }
        return builder.toString();
   }

}
