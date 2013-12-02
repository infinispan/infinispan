package org.jboss.as.clustering.infinispan.subsystem;

import org.infinispan.Cache;
import org.infinispan.interceptors.ActivationInterceptor;
import org.infinispan.interceptors.CacheMgmtInterceptor;
import org.infinispan.interceptors.CacheWriterInterceptor;
import org.infinispan.interceptors.InvalidationInterceptor;
import org.infinispan.interceptors.PassivationInterceptor;
import org.infinispan.interceptors.TxInterceptor;
import org.infinispan.remoting.rpc.RpcManagerImpl;
import org.infinispan.transaction.xa.recovery.RecoveryAdminOperations;
import org.infinispan.xsite.XSiteAdminOperations;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.OperationStepHandler;
import org.jboss.as.controller.PathAddress;
import org.jboss.dmr.ModelNode;
import org.jboss.msc.service.ServiceController;

import static org.jboss.as.clustering.infinispan.InfinispanMessages.MESSAGES;
import static org.jboss.as.clustering.infinispan.subsystem.CacheMetricsHandler.getFirstInterceptorWhichExtends;
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
            CacheMgmtInterceptor interceptor = getFirstInterceptorWhichExtends(cache.getAdvancedCache()
                    .getInterceptorChain(), CacheMgmtInterceptor.class);
            if (interceptor != null) {
                interceptor.resetStatistics();
            }
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
            cache.clear();
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
            TxInterceptor interceptor = getFirstInterceptorWhichExtends(cache.getAdvancedCache()
                    .getInterceptorChain(), TxInterceptor.class);
            if (interceptor != null) {
                interceptor.resetStatistics();
            }
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
            InvalidationInterceptor interceptor = getFirstInterceptorWhichExtends(cache.getAdvancedCache()
                    .getInterceptorChain(), InvalidationInterceptor.class);
            if (interceptor != null) {
                interceptor.resetStatistics();
            }
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
            ActivationInterceptor interceptor = getFirstInterceptorWhichExtends(cache.getAdvancedCache()
                    .getInterceptorChain(), ActivationInterceptor.class);
            if (interceptor != null) {
                interceptor.resetStatistics();
            }
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
            PassivationInterceptor interceptor = getFirstInterceptorWhichExtends(cache.getAdvancedCache()
                    .getInterceptorChain(), PassivationInterceptor.class);
            if (interceptor != null) {
                interceptor.resetStatistics();
            }
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
            RpcManagerImpl rpcManager = (RpcManagerImpl) cache.getAdvancedCache().getRpcManager();
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
            CacheWriterInterceptor interceptor = getFirstInterceptorWhichExtends(cache.getAdvancedCache()
                    .getInterceptorChain(), CacheWriterInterceptor.class);
            if (interceptor != null) {
                interceptor.resetStatistics();
            }
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
            RecoveryAdminOperations recoveryAdminOperations = cache.getAdvancedCache().getComponentRegistry().getComponent(RecoveryAdminOperations.class);
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
            RecoveryAdminOperations recoveryAdminOperations = cache.getAdvancedCache().getComponentRegistry().getComponent(RecoveryAdminOperations.class);
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
            RecoveryAdminOperations recoveryAdminOperations = cache.getAdvancedCache().getComponentRegistry().getComponent(RecoveryAdminOperations.class);
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
            RecoveryAdminOperations recoveryAdminOperations = cache.getAdvancedCache().getComponentRegistry().getComponent(RecoveryAdminOperations.class);
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
            XSiteAdminOperations xsiteAdminOperations = cache.getAdvancedCache().getComponentRegistry().getComponent(XSiteAdminOperations.class);
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
            XSiteAdminOperations xsiteAdminOperations = cache.getAdvancedCache().getComponentRegistry().getComponent(XSiteAdminOperations.class);
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
            XSiteAdminOperations xsiteAdminOperations = cache.getAdvancedCache().getComponentRegistry().getComponent(XSiteAdminOperations.class);
            return toOperationResult(xsiteAdminOperations.siteStatus(site));
        }
    }

    private static ModelNode toOperationResult(String s) {
        ModelNode result = new ModelNode();
        result.add(s);
        return result;
    }

}
