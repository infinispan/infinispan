package org.infinispan.factories;


import static org.infinispan.commons.util.Util.getInstance;

import org.infinispan.batch.BatchContainer;
import org.infinispan.cache.impl.CacheConfigurationMBean;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.CommandsFactoryImpl;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.io.ByteBufferFactory;
import org.infinispan.commons.io.ByteBufferFactoryImpl;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.context.InvocationContextFactory;
import org.infinispan.context.NonTransactionalInvocationContextFactory;
import org.infinispan.context.TransactionalInvocationContextFactory;
import org.infinispan.distribution.L1Manager;
import org.infinispan.distribution.RemoteValueRetrievedListener;
import org.infinispan.distribution.TriangleOrderManager;
import org.infinispan.distribution.impl.L1ManagerImpl;
import org.infinispan.eviction.ActivationManager;
import org.infinispan.eviction.EvictionManager;
import org.infinispan.eviction.PassivationManager;
import org.infinispan.eviction.impl.ActivationManagerImpl;
import org.infinispan.eviction.impl.EvictionManagerImpl;
import org.infinispan.eviction.impl.PassivationManagerImpl;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.functional.impl.FunctionalNotifier;
import org.infinispan.functional.impl.FunctionalNotifierImpl;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.marshall.core.MarshalledEntryFactory;
import org.infinispan.marshall.core.MarshalledEntryFactoryImpl;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.notifications.cachelistener.CacheNotifierImpl;
import org.infinispan.notifications.cachelistener.cluster.ClusterCacheNotifier;
import org.infinispan.persistence.manager.OrderedUpdatesManager;
import org.infinispan.persistence.manager.OrderedUpdatesManagerImpl;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.manager.PersistenceManagerImpl;
import org.infinispan.scattered.ScatteredVersionManager;
import org.infinispan.scattered.impl.ScatteredVersionManagerImpl;
import org.infinispan.statetransfer.CommitManager;
import org.infinispan.statetransfer.StateTransferLock;
import org.infinispan.statetransfer.StateTransferLockImpl;
import org.infinispan.transaction.impl.ClusteredTransactionOriginatorChecker;
import org.infinispan.transaction.impl.TransactionCoordinator;
import org.infinispan.transaction.impl.TransactionOriginatorChecker;
import org.infinispan.transaction.totalorder.TotalOrderManager;
import org.infinispan.transaction.xa.TransactionFactory;
import org.infinispan.transaction.xa.recovery.RecoveryAdminOperations;
import org.infinispan.util.concurrent.CommandAckCollector;
import org.infinispan.xsite.BackupSender;
import org.infinispan.xsite.BackupSenderImpl;
import org.infinispan.xsite.statetransfer.XSiteStateConsumer;
import org.infinispan.xsite.statetransfer.XSiteStateConsumerImpl;
import org.infinispan.xsite.statetransfer.XSiteStateProvider;
import org.infinispan.xsite.statetransfer.XSiteStateProviderImpl;
import org.infinispan.xsite.statetransfer.XSiteStateTransferManager;
import org.infinispan.xsite.statetransfer.XSiteStateTransferManagerImpl;

/**
 * Simple factory that just uses reflection and an empty constructor of the component type.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Pedro Ruivo
 * @since 4.0
 */
@DefaultFactoryFor(classes = {CacheNotifier.class, CacheConfigurationMBean.class, ClusterCacheNotifier.class, CommandsFactory.class,
                              PersistenceManager.class,
                              PassivationManager.class, ActivationManager.class,
                              BatchContainer.class, EvictionManager.class,
                              TransactionCoordinator.class, RecoveryAdminOperations.class, StateTransferLock.class,
                              ClusteringDependentLogic.class, L1Manager.class, TransactionFactory.class, BackupSender.class,
                              TotalOrderManager.class, ByteBufferFactory.class, MarshalledEntryFactory.class,
                              RemoteValueRetrievedListener.class, InvocationContextFactory.class, CommitManager.class,
                              XSiteStateTransferManager.class, XSiteStateConsumer.class, XSiteStateProvider.class,
                              FunctionalNotifier.class, CommandAckCollector.class, TriangleOrderManager.class,
                              OrderedUpdatesManager.class, ScatteredVersionManager.class, TransactionOriginatorChecker.class})
public class EmptyConstructorNamedCacheFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {

   @Override
   @SuppressWarnings("unchecked")
   public <T> T construct(Class<T> componentType) {
      Class<?> componentImpl;
      if (componentType.equals(ClusteringDependentLogic.class)) {
         CacheMode cacheMode = configuration.clustering().cacheMode();
         if (!cacheMode.isClustered()) {
            return componentType.cast(new ClusteringDependentLogic.LocalLogic());
         } else if (cacheMode.isInvalidation()) {
            return componentType.cast(new ClusteringDependentLogic.InvalidationLogic());
         } else if (cacheMode.isReplicated()) {
            return componentType.cast(new ClusteringDependentLogic.ReplicationLogic());
         } else if (cacheMode.isDistributed()){
            return componentType.cast(new ClusteringDependentLogic.DistributionLogic());
         } else if (cacheMode.isScattered()) {
            return componentType.cast(new ClusteringDependentLogic.ScatteredLogic());
         } else {
            throw new UnsupportedOperationException();
         }
      } else {
         boolean isTransactional = configuration.transaction().transactionMode().isTransactional();
         if (componentType.equals(InvocationContextFactory.class)) {
            componentImpl = isTransactional ? TransactionalInvocationContextFactory.class
                  : NonTransactionalInvocationContextFactory.class;
            return componentType.cast(getInstance(componentImpl));
         } else if (componentType.equals(CacheNotifier.class)) {
            return (T) new CacheNotifierImpl();
         } else if (componentType.equals(CacheConfigurationMBean.class)) {
            return (T) new CacheConfigurationMBean();
         } else if (componentType.equals(CommandsFactory.class)) {
            return (T) new CommandsFactoryImpl();
         } else if (componentType.equals(PersistenceManager.class)) {
            return (T) new PersistenceManagerImpl();
         } else if (componentType.equals(PassivationManager.class)) {
            return (T) new PassivationManagerImpl();
         } else if (componentType.equals(ActivationManager.class)) {
            return (T) new ActivationManagerImpl();
         } else if (componentType.equals(BatchContainer.class)) {
            return (T) new BatchContainer();
         } else if (componentType.equals(TransactionCoordinator.class)) {
            return (T) new TransactionCoordinator();
         } else if (componentType.equals(RecoveryAdminOperations.class)) {
            return (T) new RecoveryAdminOperations();
         } else if (componentType.equals(StateTransferLock.class)) {
            return (T) new StateTransferLockImpl();
         } else if (componentType.equals(EvictionManager.class)) {
            return (T) new EvictionManagerImpl();
         } else if (componentType.equals(L1Manager.class)) {
            return (T) new L1ManagerImpl();
         } else if (componentType.equals(TransactionFactory.class)) {
            return (T) new TransactionFactory();
         } else if (componentType.equals(BackupSender.class)) {
            return (T) new BackupSenderImpl(globalConfiguration.sites().localSite());
         } else if (componentType.equals(TotalOrderManager.class)) {
            return isTransactional && configuration.transaction().transactionProtocol().isTotalOrder() ?
                  (T) new TotalOrderManager() : null;
         } else if (componentType.equals(ByteBufferFactory.class)) {
            return (T) new ByteBufferFactoryImpl();
         } else if (componentType.equals(MarshalledEntryFactory.class)) {
            return (T) new MarshalledEntryFactoryImpl();
         } else if (componentType.equals(RemoteValueRetrievedListener.class)) {
            // L1Manager is currently only listener for remotely retrieved values
            return (T) componentRegistry.getComponent(L1Manager.class);
         } else if (componentType.equals(ClusterCacheNotifier.class)) {
            return (T) componentRegistry.getComponent(CacheNotifier.class);
         } else if (componentType.equals(CommitManager.class)) {
            return (T) new CommitManager();
         } else if (componentType.equals(XSiteStateTransferManager.class)) {
            return (T) (configuration.sites().allBackups().isEmpty() ? null : new XSiteStateTransferManagerImpl());
         } else if (componentType.equals(XSiteStateConsumer.class)) {
            return (T) new XSiteStateConsumerImpl();
         } else if (componentType.equals(XSiteStateProvider.class)) {
            return (T) new XSiteStateProviderImpl();
         } else if (componentType.equals(FunctionalNotifier.class)) {
            return (T) new FunctionalNotifierImpl<>();
         } else if (componentType.equals(CommandAckCollector.class)) {
            return componentType.cast(new CommandAckCollector());
         } else if (componentType.equals(TriangleOrderManager.class)) {
            return componentType.cast(new TriangleOrderManager(configuration.clustering().hash().numSegments()));
         } else if (componentType.equals(OrderedUpdatesManager.class)) {
            return componentType.cast(new OrderedUpdatesManagerImpl());
         } else if (componentType.equals(ScatteredVersionManager.class)) {
            return componentType.cast(new ScatteredVersionManagerImpl());
         } else if (componentType.equals(TransactionOriginatorChecker.class)) {
            return configuration.clustering().cacheMode() == CacheMode.LOCAL ?
                  componentType.cast(TransactionOriginatorChecker.LOCAL) :
                  componentType.cast(new ClusteredTransactionOriginatorChecker());
         }
      }

      throw new CacheConfigurationException("Don't know how to create a " + componentType.getName());

   }
}
