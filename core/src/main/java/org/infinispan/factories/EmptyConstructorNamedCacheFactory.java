package org.infinispan.factories;


import static org.infinispan.util.logging.Log.CONTAINER;

import org.infinispan.batch.BatchContainer;
import org.infinispan.cache.impl.CacheConfigurationMBean;
import org.infinispan.cache.impl.InvocationHelper;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.CommandsFactoryImpl;
import org.infinispan.commons.io.ByteBufferFactory;
import org.infinispan.commons.io.ByteBufferFactoryImpl;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.container.offheap.OffHeapEntryFactory;
import org.infinispan.container.offheap.OffHeapEntryFactoryImpl;
import org.infinispan.container.offheap.OffHeapMemoryAllocator;
import org.infinispan.container.offheap.UnpooledOffHeapMemoryAllocator;
import org.infinispan.container.versioning.irac.DefaultIracTombstoneManager;
import org.infinispan.container.versioning.irac.DefaultIracVersionGenerator;
import org.infinispan.container.versioning.irac.IracTombstoneManager;
import org.infinispan.container.versioning.irac.IracVersionGenerator;
import org.infinispan.container.versioning.irac.NoOpIracTombstoneManager;
import org.infinispan.container.versioning.irac.NoOpIracVersionGenerator;
import org.infinispan.context.InvocationContextFactory;
import org.infinispan.context.impl.NonTransactionalInvocationContextFactory;
import org.infinispan.context.impl.TransactionalInvocationContextFactory;
import org.infinispan.distribution.L1Manager;
import org.infinispan.distribution.RemoteValueRetrievedListener;
import org.infinispan.distribution.TriangleOrderManager;
import org.infinispan.distribution.impl.L1ManagerImpl;
import org.infinispan.encoding.impl.StorageConfigurationManager;
import org.infinispan.eviction.EvictionManager;
import org.infinispan.eviction.impl.ActivationManager;
import org.infinispan.eviction.impl.ActivationManagerImpl;
import org.infinispan.eviction.impl.EvictionManagerImpl;
import org.infinispan.eviction.impl.PassivationManager;
import org.infinispan.eviction.impl.PassivationManagerImpl;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.impl.ComponentAlias;
import org.infinispan.functional.impl.FunctionalNotifier;
import org.infinispan.functional.impl.FunctionalNotifierImpl;
import org.infinispan.marshall.persistence.impl.MarshalledEntryFactoryImpl;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.notifications.cachelistener.CacheNotifierImpl;
import org.infinispan.notifications.cachelistener.cluster.ClusterCacheNotifier;
import org.infinispan.persistence.manager.PassivationPersistenceManager;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.manager.PersistenceManagerImpl;
import org.infinispan.persistence.manager.PreloadManager;
import org.infinispan.persistence.spi.MarshallableEntryFactory;
import org.infinispan.reactive.publisher.impl.PublisherHandler;
import org.infinispan.statetransfer.CommitManager;
import org.infinispan.statetransfer.StateTransferLock;
import org.infinispan.statetransfer.StateTransferLockImpl;
import org.infinispan.transaction.impl.ClusteredTransactionOriginatorChecker;
import org.infinispan.transaction.impl.TransactionCoordinator;
import org.infinispan.transaction.impl.TransactionOriginatorChecker;
import org.infinispan.transaction.xa.TransactionFactory;
import org.infinispan.transaction.xa.recovery.RecoveryAdminOperations;
import org.infinispan.util.concurrent.CommandAckCollector;
import org.infinispan.xsite.BackupReceiver;
import org.infinispan.xsite.BackupSender;
import org.infinispan.xsite.BackupSenderImpl;
import org.infinispan.xsite.ClusteredCacheBackupReceiver;
import org.infinispan.xsite.NoOpBackupSender;
import org.infinispan.xsite.irac.DefaultIracManager;
import org.infinispan.xsite.irac.IracManager;
import org.infinispan.xsite.irac.NoOpIracManager;
import org.infinispan.xsite.metrics.DefaultXSiteMetricsCollector;
import org.infinispan.xsite.metrics.NoOpXSiteMetricsCollector;
import org.infinispan.xsite.metrics.XSiteMetricsCollector;
import org.infinispan.xsite.statetransfer.NoOpXSiteStateProvider;
import org.infinispan.xsite.statetransfer.NoOpXSiteStateTransferManager;
import org.infinispan.xsite.statetransfer.XSiteStateConsumer;
import org.infinispan.xsite.statetransfer.XSiteStateConsumerImpl;
import org.infinispan.xsite.statetransfer.XSiteStateProvider;
import org.infinispan.xsite.statetransfer.XSiteStateProviderImpl;
import org.infinispan.xsite.statetransfer.XSiteStateTransferManager;
import org.infinispan.xsite.statetransfer.XSiteStateTransferManagerImpl;
import org.infinispan.xsite.status.DefaultTakeOfflineManager;
import org.infinispan.xsite.status.NoOpTakeOfflineManager;
import org.infinispan.xsite.status.TakeOfflineManager;

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
                              ByteBufferFactory.class, MarshallableEntryFactory.class,
                              RemoteValueRetrievedListener.class, InvocationContextFactory.class, CommitManager.class,
                              XSiteStateTransferManager.class, XSiteStateConsumer.class, XSiteStateProvider.class,
                              FunctionalNotifier.class, CommandAckCollector.class, TriangleOrderManager.class,
                              TransactionOriginatorChecker.class, OffHeapEntryFactory.class, OffHeapMemoryAllocator.class,
                              PublisherHandler.class, InvocationHelper.class, TakeOfflineManager.class, IracManager.class,
                              IracVersionGenerator.class, BackupReceiver.class, StorageConfigurationManager.class,
                              XSiteMetricsCollector.class, IracTombstoneManager.class
})
public class EmptyConstructorNamedCacheFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {

   @Override
   public Object construct(String componentName) {
      boolean isTransactional = configuration.transaction().transactionMode().isTransactional();
      if (componentName.equals(InvocationContextFactory.class.getName())) {
         return isTransactional ? new TransactionalInvocationContextFactory()
               : new NonTransactionalInvocationContextFactory();
      } else if (componentName.equals(CacheNotifier.class.getName())) {
         return new CacheNotifierImpl<>();
      } else if (componentName.equals(CacheConfigurationMBean.class.getName())) {
         return new CacheConfigurationMBean();
      } else if (componentName.equals(CommandsFactory.class.getName())) {
         return new CommandsFactoryImpl();
      } else if (componentName.equals(PersistenceManager.class.getName())) {
         PersistenceManagerImpl persistenceManager = new PersistenceManagerImpl();
         if (configuration.persistence().passivation()) {
            return new PassivationPersistenceManager(persistenceManager);
         }
         return persistenceManager;
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
         return new EvictionManagerImpl<>();
      } else if (componentName.equals(L1Manager.class.getName())) {
         return new L1ManagerImpl();
      } else if (componentName.equals(TransactionFactory.class.getName())) {
         return new TransactionFactory();
      } else if (componentName.equals(BackupSender.class.getName())) {
         return configuration.sites().hasSyncEnabledBackups() ?
                new BackupSenderImpl() :
                NoOpBackupSender.getInstance();
      } else if (componentName.equals(ByteBufferFactory.class.getName())) {
         return new ByteBufferFactoryImpl();
      } else if (componentName.equals(MarshallableEntryFactory.class.getName())) {
         return new MarshalledEntryFactoryImpl();
      } else if (componentName.equals(CommitManager.class.getName())) {
         return new CommitManager();
      } else if (componentName.equals(XSiteStateTransferManager.class.getName())) {
         return configuration.sites().hasBackups() ? new XSiteStateTransferManagerImpl(configuration)
                                                          : new NoOpXSiteStateTransferManager();
      } else if (componentName.equals(XSiteStateConsumer.class.getName())) {
         return new XSiteStateConsumerImpl(configuration);
      } else if (componentName.equals(XSiteStateProvider.class.getName())) {
         return configuration.sites().hasBackups() ? new XSiteStateProviderImpl(configuration)
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
      } else if (componentName.equals(TransactionOriginatorChecker.class.getName())) {
         return configuration.clustering().cacheMode() == CacheMode.LOCAL ?
               TransactionOriginatorChecker.LOCAL :
               new ClusteredTransactionOriginatorChecker();
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
      } else if (componentName.equals(InvocationHelper.class.getName())) {
         return new InvocationHelper();
      } else if (componentName.equals(TakeOfflineManager.class.getName())) {
         return configuration.sites().hasBackups() ?
               new DefaultTakeOfflineManager(componentRegistry.getCacheName()) :
               NoOpTakeOfflineManager.getInstance();
      } else if (componentName.equals(IracManager.class.getName())) {
         return configuration.sites().hasAsyncEnabledBackups() ?
                new DefaultIracManager(configuration) :
                NoOpIracManager.INSTANCE;
      } else if (componentName.equals(IracVersionGenerator.class.getName())) {
         return configuration.sites().hasAsyncEnabledBackups() ?
                new DefaultIracVersionGenerator(configuration.clustering().hash().numSegments()) :
                NoOpIracVersionGenerator.getInstance();
      } else if (componentName.equals(BackupReceiver.class.getName())) {
         return configuration.clustering().cacheMode().isClustered() ?
               new ClusteredCacheBackupReceiver(componentRegistry.getCacheName()) :
               null;
      } else if (componentName.equals(StorageConfigurationManager.class.getName())) {
         return new StorageConfigurationManager();
      } else if (componentName.equals(XSiteMetricsCollector.class.getName())) {
         return configuration.sites().hasBackups() ?
                new DefaultXSiteMetricsCollector(configuration) :
                NoOpXSiteMetricsCollector.getInstance();
      } else if (componentName.equals(IracTombstoneManager.class.getName())) {
         return configuration.sites().hasAsyncEnabledBackups() ?
               new DefaultIracTombstoneManager(configuration) :
               NoOpIracTombstoneManager.getInstance();
      }

      throw CONTAINER.factoryCannotConstructComponent(componentName);
   }
}
