/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2012, Red Hat, Inc., and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.jboss.as.clustering.infinispan.subsystem;

import static org.jboss.as.clustering.infinispan.InfinispanMessages.MESSAGES;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP_ADDR;

import java.util.Iterator;
import java.util.ListIterator;
import java.util.Map;

import org.infinispan.Cache;
import org.infinispan.eviction.PassivationManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.interceptors.impl.CacheLoaderInterceptor;
import org.infinispan.interceptors.impl.CacheMgmtInterceptor;
import org.infinispan.interceptors.impl.CacheWriterInterceptor;
import org.infinispan.interceptors.impl.InvalidationInterceptor;
import org.infinispan.interceptors.impl.TxInterceptor;
import org.infinispan.query.SearchManager;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcManagerImpl;
import org.infinispan.server.infinispan.SecurityActions;
import org.infinispan.server.infinispan.spi.InfinispanSubsystem;
import org.infinispan.server.infinispan.spi.service.CacheServiceName;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.transaction.xa.recovery.RecoveryAdminOperations;
import org.infinispan.upgrade.RollingUpgradeManager;
import org.infinispan.xsite.XSiteAdminOperations;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.OperationStepHandler;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.PathElement;
import org.jboss.dmr.ModelNode;
import org.jboss.msc.service.ServiceController;

/**
 * Custom commands related to a cache
 *
 * @author Tristan Tarrant
 */
public abstract class CacheCommands implements OperationStepHandler {

    CacheCommands() {
    }

    /**
     * An attribute write handler which performs cache operations
     *
     * @param context the operation context
     * @param operation the operation being executed
     * @throws org.jboss.as.controller.OperationFailedException
     */
    @Override
    public void execute(OperationContext context, ModelNode operation) throws OperationFailedException {
        final PathAddress address = PathAddress.pathAddress(operation.require(OP_ADDR));
        ListIterator<PathElement> iterator = address.iterator();
        PathElement element = iterator.next();
        while (!element.getValue().equals(InfinispanSubsystem.SUBSYSTEM_NAME)) {
            element = iterator.next();
        }
        final String cacheContainerName = iterator.next().getValue();
        final String cacheName = iterator.next().getValue();
        if (context.isNormalServer()) {
            final ServiceController<?> controller = context.getServiceRegistry(false).getService(CacheServiceName.CACHE.getServiceName(cacheContainerName, cacheName));
            Cache<?, ?> cache = (Cache<?, ?>) controller.getValue();

            ModelNode operationResult = null;
            try {
                operationResult = invokeCommand(cache, operation, context);
            } catch (Exception e) {
                throw new OperationFailedException(MESSAGES.failedToInvokeOperation(e.getLocalizedMessage()), e);
            }
            if (operationResult != null) {
                context.getResult().set(operationResult);
            }
        }
    }

    protected abstract ModelNode invokeCommand(Cache<?, ?> cache, ModelNode operation, OperationContext operationContext) throws Exception;

    public static class ResetCacheStatisticsCommand extends CacheCommands {
        public static final ResetCacheStatisticsCommand INSTANCE = new ResetCacheStatisticsCommand();

        public ResetCacheStatisticsCommand() {
        }

        @Override
        protected ModelNode invokeCommand(Cache<?, ?> cache, ModelNode operation, OperationContext context) throws Exception {
            SecurityActions.resetStatistics(cache.getAdvancedCache(), CacheMgmtInterceptor.class);
            return null;
        }
    }

    public static class ClearCacheCommand extends CacheCommands {
        public static final ClearCacheCommand INSTANCE = new ClearCacheCommand();

        public ClearCacheCommand() {
        }

        @Override
        protected ModelNode invokeCommand(Cache<?, ?> cache, ModelNode operation, OperationContext context) throws Exception {
            SecurityActions.clearCache(cache.getAdvancedCache());
            return null;
        }
    }

    public static class FlushCacheCommand extends CacheCommands {
        public static final FlushCacheCommand INSTANCE = new FlushCacheCommand();

        public FlushCacheCommand() {
        }

        @Override
        protected ModelNode invokeCommand(Cache<?, ?> cache, ModelNode operation, OperationContext context) throws Exception {
            SecurityActions.flushCache(cache.getAdvancedCache());
            return null;
        }
    }

    public static class StartCacheCommand extends CacheCommands {
        public static final StartCacheCommand INSTANCE = new StartCacheCommand();

        public StartCacheCommand() {
        }

        @Override
        protected ModelNode invokeCommand(Cache<?, ?> cache, ModelNode operation, OperationContext context) throws Exception {
            SecurityActions.startCache(cache.getAdvancedCache());
            return null;
        }
    }

    public static class StopCacheCommand extends CacheCommands {
        public static final StopCacheCommand INSTANCE = new StopCacheCommand();

        public StopCacheCommand() {
        }

        @Override
        protected ModelNode invokeCommand(Cache<?, ?> cache, ModelNode operation, OperationContext context) throws Exception {
            SecurityActions.stopCache(cache.getAdvancedCache());
            return null;
        }
    }

    public static class ShudownCacheCommand extends CacheCommands {
        public static final ShudownCacheCommand INSTANCE = new ShudownCacheCommand();

        public ShudownCacheCommand() {
        }

        @Override
        protected ModelNode invokeCommand(Cache<?, ?> cache, ModelNode operation, OperationContext context) throws Exception {
            SecurityActions.shutdownCache(cache.getAdvancedCache());
            return null;
        }
    }

    public static class ResetTxStatisticsCommand extends CacheCommands {
        public static final ResetTxStatisticsCommand INSTANCE = new ResetTxStatisticsCommand();

        public ResetTxStatisticsCommand() {
        }

        @Override
        protected ModelNode invokeCommand(Cache<?, ?> cache, ModelNode operation, OperationContext context) throws Exception {
            SecurityActions.resetStatistics(cache.getAdvancedCache(), TxInterceptor.class);
            return null;
        }
    }

    public static class ResetInvalidationStatisticsCommand extends CacheCommands {
        public static final ResetInvalidationStatisticsCommand INSTANCE = new ResetInvalidationStatisticsCommand();

        public ResetInvalidationStatisticsCommand() {
        }

        @Override
        protected ModelNode invokeCommand(Cache<?, ?> cache, ModelNode operation, OperationContext context) throws Exception {
            SecurityActions.resetStatistics(cache.getAdvancedCache(), InvalidationInterceptor.class);
            return null;
        }
    }

    public static class ResetActivationStatisticsCommand extends CacheCommands {
        public static final ResetActivationStatisticsCommand INSTANCE = new ResetActivationStatisticsCommand();

        public ResetActivationStatisticsCommand() {
        }

        @Override
        protected ModelNode invokeCommand(Cache<?, ?> cache, ModelNode operation, OperationContext context) throws Exception {
            SecurityActions.resetStatistics(cache.getAdvancedCache(), CacheLoaderInterceptor.class);
            return null;
        }
    }

    public static class ResetPassivationStatisticsCommand extends CacheCommands {
        public static final ResetPassivationStatisticsCommand INSTANCE = new ResetPassivationStatisticsCommand();

        @Override
        protected ModelNode invokeCommand(Cache<?, ?> cache, ModelNode operation, OperationContext context) throws Exception {
            SecurityActions.resetStatistics(cache.getAdvancedCache(), PassivationManager.class);
            return null;
        }
    }

    public static class ResetRpcManagerStatisticsCommand extends CacheCommands {
        public static final ResetRpcManagerStatisticsCommand INSTANCE = new ResetRpcManagerStatisticsCommand();

        @Override
        protected ModelNode invokeCommand(Cache<?, ?> cache, ModelNode operation, OperationContext context) throws Exception {
            RpcManagerImpl rpcManager = (RpcManagerImpl) SecurityActions.getComponentRegistry(cache.getAdvancedCache()).getComponent(RpcManager.class);
            if (rpcManager != null) {
                rpcManager.resetStatistics();
            }
            return null;
        }
    }

    public static class ResetCacheLoaderStatisticsCommand extends CacheCommands {
        public static final ResetCacheLoaderStatisticsCommand INSTANCE = new ResetCacheLoaderStatisticsCommand();

        @Override
        protected ModelNode invokeCommand(Cache<?, ?> cache, ModelNode operation, OperationContext context) throws Exception {
            SecurityActions.resetStatistics(cache.getAdvancedCache(), CacheWriterInterceptor.class);
            return null;
        }
    }

    public static class TransactionListInDoubtCommand extends CacheCommands {
        public static final TransactionListInDoubtCommand INSTANCE = new TransactionListInDoubtCommand();

        @Override
        protected ModelNode invokeCommand(Cache<?, ?> cache, ModelNode operation, OperationContext context) throws Exception {
            RecoveryAdminOperations recoveryAdminOperations = SecurityActions.getComponentRegistry(cache.getAdvancedCache()).getComponent(RecoveryAdminOperations.class);
            return toOperationResult(recoveryAdminOperations.showInDoubtTransactions());
        }
    }

    public static class TransactionForceCommitCommand extends CacheCommands {
        public static final TransactionForceCommitCommand INSTANCE = new TransactionForceCommitCommand();

        @Override
        protected ModelNode invokeCommand(Cache<?, ?> cache, ModelNode operation, OperationContext context) throws Exception {
            long internalId = operation.require(ModelKeys.TX_INTERNAL_ID).asLong();
            RecoveryAdminOperations recoveryAdminOperations = SecurityActions.getComponentRegistry(cache.getAdvancedCache()).getComponent(RecoveryAdminOperations.class);
            return toOperationResult(recoveryAdminOperations.forceCommit(internalId));
        }
    }

    public static class TransactionForceRollbackCommand extends CacheCommands {
        public static final TransactionForceRollbackCommand INSTANCE = new TransactionForceRollbackCommand();

        @Override
        protected ModelNode invokeCommand(Cache<?, ?> cache, ModelNode operation, OperationContext context) throws Exception {
            long internalId = operation.require(ModelKeys.TX_INTERNAL_ID).asLong();
            RecoveryAdminOperations recoveryAdminOperations = SecurityActions.getComponentRegistry(cache.getAdvancedCache()).getComponent(RecoveryAdminOperations.class);
            return toOperationResult(recoveryAdminOperations.forceRollback(internalId));
        }
    }

    public static class TransactionForgetCommand extends CacheCommands {
        public static final TransactionForgetCommand INSTANCE = new TransactionForgetCommand();

        @Override
        protected ModelNode invokeCommand(Cache<?, ?> cache, ModelNode operation, OperationContext context) throws Exception {
            long internalId = operation.require(ModelKeys.TX_INTERNAL_ID).asLong();
            RecoveryAdminOperations recoveryAdminOperations = SecurityActions.getComponentRegistry(cache.getAdvancedCache()).getComponent(RecoveryAdminOperations.class);
            return toOperationResult(recoveryAdminOperations.forget(internalId));
        }
    }

    public static class BackupBringSiteOnlineCommand extends CacheCommands {
        public static final BackupBringSiteOnlineCommand INSTANCE = new BackupBringSiteOnlineCommand();

        @Override
        protected ModelNode invokeCommand(Cache<?, ?> cache, ModelNode operation, OperationContext context) throws Exception {
            final PathAddress address = PathAddress.pathAddress(operation.require(OP_ADDR));
            final String site = address.getLastElement().getValue();
            XSiteAdminOperations xsiteAdminOperations = SecurityActions.getComponentRegistry(cache.getAdvancedCache()).getComponent(XSiteAdminOperations.class);
            return toOperationResult(xsiteAdminOperations.bringSiteOnline(site));
        }
    }

    public static class BackupTakeSiteOfflineCommand extends CacheCommands {
        public static final BackupTakeSiteOfflineCommand INSTANCE = new BackupTakeSiteOfflineCommand();

        @Override
        protected ModelNode invokeCommand(Cache<?, ?> cache, ModelNode operation, OperationContext context) throws Exception {
            final PathAddress address = PathAddress.pathAddress(operation.require(OP_ADDR));
            final String site = address.getLastElement().getValue();
            XSiteAdminOperations xsiteAdminOperations = SecurityActions.getComponentRegistry(cache.getAdvancedCache()).getComponent(XSiteAdminOperations.class);
            return toOperationResult(xsiteAdminOperations.takeSiteOffline(site));
        }
    }

    public static class BackupSiteStatusCommand extends CacheCommands {
        public static final BackupSiteStatusCommand INSTANCE = new BackupSiteStatusCommand();

        @Override
        protected ModelNode invokeCommand(Cache<?, ?> cache, ModelNode operation, OperationContext context) throws Exception {
            final PathAddress address = PathAddress.pathAddress(operation.require(OP_ADDR));
            final String site = address.getLastElement().getValue();
            XSiteAdminOperations xsiteAdminOperations = SecurityActions.getComponentRegistry(cache.getAdvancedCache()).getComponent(XSiteAdminOperations.class);
            return toOperationResult(xsiteAdminOperations.siteStatus(site));
        }
    }

    public static class SynchronizeDataCommand extends CacheCommands {
        public static final SynchronizeDataCommand INSTANCE = new SynchronizeDataCommand();

        @Override
        protected ModelNode invokeCommand(Cache<?, ?> cache, ModelNode operation, OperationContext context) throws Exception {
            ComponentRegistry registry = SecurityActions.getComponentRegistry(cache.getAdvancedCache());
            RollingUpgradeManager manager = registry.getComponent(RollingUpgradeManager.class);
            if (manager != null) {
                int readBatch = CacheResource.READ_BATCH.resolveModelAttribute(context,operation).asInt();
                int writeThreads = CacheResource.WRITE_THREADS.resolveModelAttribute(context,operation).asInt();
                manager.synchronizeData(operation.require(ModelKeys.MIGRATOR_NAME).asString(), readBatch, writeThreads);
            }
            return null;
        }
    }

    public static class DisconnectSourceCommand extends CacheCommands {
        public static final DisconnectSourceCommand INSTANCE = new DisconnectSourceCommand();

        @Override
        protected ModelNode invokeCommand(Cache<?, ?> cache, ModelNode operation, OperationContext context) throws Exception {
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

        @Override
        protected ModelNode invokeCommand(Cache<?, ?> cache, ModelNode operation, OperationContext context) throws Exception {
            final PathAddress address = PathAddress.pathAddress(operation.require(OP_ADDR));
            final String site = address.getLastElement().getValue();
            XSiteAdminOperations xsiteAdminOperations = SecurityActions.getComponentRegistry(cache.getAdvancedCache()).getComponent(XSiteAdminOperations.class);
            return toOperationResult(xsiteAdminOperations.pushState(site));
        }
    }

    public static class BackupCancelPushStateCommand extends CacheCommands {
        public static final BackupCancelPushStateCommand INSTANCE = new BackupCancelPushStateCommand();

        @Override
        protected ModelNode invokeCommand(Cache<?, ?> cache, ModelNode operation, OperationContext context) throws Exception {
            final PathAddress address = PathAddress.pathAddress(operation.require(OP_ADDR));
            final String site = address.getLastElement().getValue();
            XSiteAdminOperations xsiteAdminOperations = SecurityActions.getComponentRegistry(cache.getAdvancedCache()).getComponent(XSiteAdminOperations.class);
            return toOperationResult(xsiteAdminOperations.cancelPushState(site));
        }
    }

    public static class BackupCancelReceiveStateCommand extends CacheCommands {
        public static final BackupCancelReceiveStateCommand INSTANCE = new BackupCancelReceiveStateCommand();

        @Override
        protected ModelNode invokeCommand(Cache<?, ?> cache, ModelNode operation, OperationContext context) throws Exception {
            final PathAddress address = PathAddress.pathAddress(operation.require(OP_ADDR));
            final String site = address.getLastElement().getValue();
            XSiteAdminOperations xsiteAdminOperations = SecurityActions.getComponentRegistry(cache.getAdvancedCache()).getComponent(XSiteAdminOperations.class);
            return toOperationResult(xsiteAdminOperations.cancelReceiveState(site));
        }
    }

    public static class BackupPushStateStatusCommand extends CacheCommands {
        public static final BackupPushStateStatusCommand INSTANCE = new BackupPushStateStatusCommand();

        @Override
        protected ModelNode invokeCommand(Cache<?, ?> cache, ModelNode operation, OperationContext context) throws Exception {
            XSiteAdminOperations xsiteAdminOperations = SecurityActions.getComponentRegistry(cache.getAdvancedCache()).getComponent(XSiteAdminOperations.class);
            return toOperationResult(prettyPrintMap(xsiteAdminOperations.getPushStateStatus()));
        }
    }

    public static class BackupGetSendingSiteCommand extends CacheCommands {
        public static final BackupGetSendingSiteCommand INSTANCE = new BackupGetSendingSiteCommand();

        @Override
        protected ModelNode invokeCommand(Cache<?, ?> cache, ModelNode operation, OperationContext context) throws Exception {
            XSiteAdminOperations xsiteAdminOperations = SecurityActions.getComponentRegistry(cache.getAdvancedCache()).getComponent(XSiteAdminOperations.class);
            return toOperationResult(xsiteAdminOperations.getSendingSiteName());
        }
    }

    public static class BackupClearPushStatusCommand extends CacheCommands {
        public static final BackupClearPushStatusCommand INSTANCE = new BackupClearPushStatusCommand();

        @Override
        protected ModelNode invokeCommand(Cache<?, ?> cache, ModelNode operation, OperationContext context) throws Exception {
            XSiteAdminOperations xsiteAdminOperations = SecurityActions.getComponentRegistry(cache.getAdvancedCache()).getComponent(XSiteAdminOperations.class);
            xsiteAdminOperations.clearPushStateStatus();
            return null;
        }
    }

    public static class MassReindexCommand extends CacheCommands {
        public static final MassReindexCommand INSTANCE = new MassReindexCommand();


        @Override
        protected ModelNode invokeCommand(Cache<?, ?> cache, ModelNode operation, OperationContext context) throws Exception {
            SearchManager searchManager = SecurityActions.getSearchManager(cache.getAdvancedCache());
            if (searchManager != null) {
                searchManager.getMassIndexer().start();
            }
            return null;
        }
    }

    public static class CacheRebalanceCommand extends CacheCommands {

        public static final CacheRebalanceCommand INSTANCE = new CacheRebalanceCommand();

        @Override
        protected ModelNode invokeCommand(Cache<?, ?> cache, ModelNode operation, OperationContext operationContext) throws Exception {
            boolean value = SharedCacheResource.BOOL_VALUE.resolveModelAttribute(operationContext, operation).asBoolean();
            LocalTopologyManager topologyManager = SecurityActions.getComponentRegistry(cache.getAdvancedCache())
                  .getComponent(LocalTopologyManager.class);
            if (topologyManager != null) {
                topologyManager.setCacheRebalancingEnabled(cache.getName(), value);
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
