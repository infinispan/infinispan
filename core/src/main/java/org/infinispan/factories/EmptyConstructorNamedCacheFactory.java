package org.infinispan.factories;


import static org.infinispan.util.logging.Log.CONTAINER;

import org.infinispan.batch.BatchContainer;
import org.infinispan.cache.impl.CacheConfigurationMBean;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.CommandsFactoryImpl;
import org.infinispan.commons.io.ByteBufferFactory;
import org.infinispan.commons.io.ByteBufferFactoryImpl;
import org.infinispan.configuration.cache.BiasAcquisition;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.container.offheap.OffHeapEntryFactory;
import org.infinispan.container.offheap.OffHeapEntryFactoryImpl;
import org.infinispan.container.offheap.OffHeapMemoryAllocator;
import org.infinispan.container.offheap.UnpooledOffHeapMemoryAllocator;
import org.infinispan.context.InvocationContextFactory;
import org.infinispan.context.impl.NonTransactionalInvocationContextFactory;
import org.infinispan.context.impl.TransactionalInvocationContextFactory;
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
import org.infinispan.factories.impl.ComponentAlias;
import org.infinispan.functional.impl.FunctionalNotifier;
import org.infinispan.functional.impl.FunctionalNotifierImpl;
import org.infinispan.marshall.core.MarshalledEntryFactory;
import org.infinispan.marshall.persistence.impl.MarshalledEntryFactoryImpl;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.notifications.cachelistener.CacheNotifierImpl;
import org.infinispan.notifications.cachelistener.cluster.ClusterCacheNotifier;
import org.infinispan.persistence.manager.OrderedUpdatesManager;
import org.infinispan.persistence.manager.OrderedUpdatesManagerImpl;
import org.infinispan.persistence.manager.PassivationPersistenceManager;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.manager.PersistenceManagerImpl;
import org.infinispan.persistence.manager.PersistenceManagerStub;
import org.infinispan.persistence.manager.PreloadManager;
import org.infinispan.persistence.spi.MarshallableEntryFactory;
import org.infinispan.reactive.publisher.impl.PublisherHandler;
import org.infinispan.scattered.BiasManager;
import org.infinispan.scattered.ScatteredVersionManager;
import org.infinispan.scattered.impl.BiasManagerImpl;
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
import org.infinispan.xsite.NoOpBackupSender;
import org.infinispan.xsite.statetransfer.NoOpXSiteStateProvider;
import org.infinispan.xsite.statetransfer.NoOpXSiteStateTransferManager;
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
                              PersistenceManager.class, PassivationManager.class, ActivationManager.class,
                              PreloadManager.class, BatchContainer.class, EvictionManager.class,
                              TransactionCoordinator.class, RecoveryAdminOperations.class, StateTransferLock.class,
                              L1Manager.class, TransactionFactory.class, BackupSender.class,
                              TotalOrderManager.class, ByteBufferFactory.class, MarshalledEntryFactory.class, MarshallableEntryFactory.class,
                              RemoteValueRetrievedListener.class, InvocationContextFactory.class, CommitManager.class,
                              XSiteStateTransferManager.class, XSiteStateConsumer.class, XSiteStateProvider.class,
                              FunctionalNotifier.class, CommandAckCollector.class, TriangleOrderManager.class,
                              OrderedUpdatesManager.class, ScatteredVersionManager.class, TransactionOriginatorChecker.class,
                              BiasManager.class, OffHeapEntryFactory.class, OffHeapMemoryAllocator.class, PublisherHandler.class})
public class EmptyConstructorNamedCacheFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {

   @Override
   public Object construct(String componentName) {
      boolean isTransactional = configuration.transaction().transactionMode().isTransactional();
      if (componentName.equals(InvocationContextFactory.class.getName())) {
         return isTransactional ? new TransactionalInvocationContextFactory()
               : new NonTransactionalInvocationContextFactory();
      } else if (componentName.equals(CacheNotifier.class.getName())) {
         return new CacheNotifierImpl();
      } else if (componentName.equals(CacheConfigurationMBean.class.getName())) {
         return new CacheConfigurationMBean();
      } else if (componentName.equals(CommandsFactory.class.getName())) {
         return new CommandsFactoryImpl();
      } else if (componentName.equals(PersistenceManager.class.getName())) {
         if (configuration.persistence().usingStores()) {
            PersistenceManagerImpl realPersistenceManager = new PersistenceManagerImpl();
            if (configuration.persistence().passivation()) {
               return new PassivationPersistenceManager(realPersistenceManager);
            }
            return realPersistenceManager;
         }
         return new PersistenceManagerStub();
      } else if (componentName.equals(PassivationManager.class.getName())) {
         return new PassivationManagerImpl();
      } else if (componentName.equals(ActivationManager.class.getName())) {
         return new ActivationManagerImpl();
      } else if (componentName.equals(PreloadManager.class.getName())) {
         return new PreloadManager();
      } else if (componentName.equals(BatchContainer.class.getName())) {
         return new BatchContainer();
      } else if (componentName.equals(TransactionCoordinator.class.getName())) {
         return new TransactionCoordinator();
      } else if (componentName.equals(RecoveryAdminOperations.class.getName())) {
         return new RecoveryAdminOperations();
      } else if (componentName.equals(StateTransferLock.class.getName())) {
         return new StateTransferLockImpl();
      } else if (componentName.equals(EvictionManager.class.getName())) {
         return new EvictionManagerImpl();
      } else if (componentName.equals(L1Manager.class.getName())) {
         return new L1ManagerImpl();
      } else if (componentName.equals(TransactionFactory.class.getName())) {
         return new TransactionFactory();
      } else if (componentName.equals(BackupSender.class.getName())) {
         return configuration.sites().hasEnabledBackups() ?
                new BackupSenderImpl(globalConfiguration.sites().localSite()) :
                NoOpBackupSender.getInstance();
      } else if (componentName.equals(TotalOrderManager.class.getName())) {
         return isTransactional && configuration.transaction().transactionProtocol().isTotalOrder() ?
               new TotalOrderManager() : null;
      } else if (componentName.equals(ByteBufferFactory.class.getName())) {
         return new ByteBufferFactoryImpl();
      } else if (componentName.equals(MarshallableEntryFactory.class.getName()) || componentName.equals(MarshalledEntryFactory.class.getName())) {
         return new MarshalledEntryFactoryImpl();
      } else if (componentName.equals(CommitManager.class.getName())) {
         return new CommitManager();
      } else if (componentName.equals(XSiteStateTransferManager.class.getName())) {
         return configuration.sites().hasEnabledBackups() ? new XSiteStateTransferManagerImpl()
                                                          : new NoOpXSiteStateTransferManager();
      } else if (componentName.equals(XSiteStateConsumer.class.getName())) {
         return new XSiteStateConsumerImpl();
      } else if (componentName.equals(XSiteStateProvider.class.getName())) {
         return configuration.sites().hasEnabledBackups() ? new XSiteStateProviderImpl()
                                                          : NoOpXSiteStateProvider.getInstance();
      } else if (componentName.equals(FunctionalNotifier.class.getName())) {
         return new FunctionalNotifierImpl<>();
      } else if (componentName.equals(CommandAckCollector.class.getName())) {
         if (configuration.clustering().cacheMode().isClustered()) {
            return new CommandAckCollector();
         } else {
            return null;
         }
      } else if (componentName.equals(TriangleOrderManager.class.getName())) {
         if (configuration.clustering().cacheMode().isClustered()) {
            return new TriangleOrderManager(configuration.clustering().hash().numSegments());
         } else {
            return null;
         }
      } else if (componentName.equals(OrderedUpdatesManager.class.getName())) {
         if (configuration.clustering().cacheMode().isScattered()) {
            return new OrderedUpdatesManagerImpl();
         } else {
            return null;
         }
      } else if (componentName.equals(ScatteredVersionManager.class.getName())) {
         if (configuration.clustering().cacheMode().isScattered()) {
            return new ScatteredVersionManagerImpl();
         } else {
            return null;
         }
      } else if (componentName.equals(TransactionOriginatorChecker.class.getName())) {
         return configuration.clustering().cacheMode() == CacheMode.LOCAL ?
               TransactionOriginatorChecker.LOCAL :
               new ClusteredTransactionOriginatorChecker();
      } else if (componentName.equals(BiasManager.class.getName())) {
         if (configuration.clustering().cacheMode().isScattered() &&
               configuration.clustering().biasAcquisition() != BiasAcquisition.NEVER) {
            return new BiasManagerImpl();
         } else {
            return null;
         }
      } else if (componentName.equals(OffHeapEntryFactory.class.getName())) {
         return new OffHeapEntryFactoryImpl();
      } else if (componentName.equals(OffHeapMemoryAllocator.class.getName())) {
         return new UnpooledOffHeapMemoryAllocator();
      } else if (componentName.equals(ClusterCacheNotifier.class.getName())) {
         return ComponentAlias.of(CacheNotifier.class);
      } else if (componentName.equals(RemoteValueRetrievedListener.class.getName())) {
         // L1Manager is currently only listener for remotely retrieved values
         return ComponentAlias.of(L1Manager.class);
      } else if (componentName.equals(PublisherHandler.class.getName())) {
         return new PublisherHandler();
      }

      throw CONTAINER.factoryCannotConstructComponent(componentName);
   }
}
