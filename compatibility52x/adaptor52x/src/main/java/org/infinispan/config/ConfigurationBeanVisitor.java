package org.infinispan.config;

import org.infinispan.config.Configuration.*;
import org.infinispan.config.GlobalConfiguration.AdvancedExternalizersType;
import org.infinispan.config.GlobalConfiguration.FactoryClassWithPropertiesType;
import org.infinispan.config.GlobalConfiguration.GlobalJmxStatisticsType;
import org.infinispan.config.GlobalConfiguration.SerializationType;
import org.infinispan.config.GlobalConfiguration.ShutdownType;
import org.infinispan.config.GlobalConfiguration.TransportType;
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.loaders.decorators.AsyncStoreConfig;
import org.infinispan.loaders.decorators.SingletonStoreConfig;

/**
 * ConfigurationBeanVisitor implementations are passed through InfinispanConfiguration object tree
 * visiting each configuration element of InfinispanConfiguration instance.
 * <p>
 * 
 * AbstractConfigurationBeanVisitor is a convenience super class for all implementations of
 * ConfigurationBeanVisitor. Most of the time, custom visitors should extend
 * AbstractConfigurationBeanVisitor rather than implement ConfigurationBeanVisitor
 * 
 * 
 * 
 * @author Vladimir Blagojevic
 * @see AbstractConfigurationBeanVisitor
 * @since 4.0
 */
public interface ConfigurationBeanVisitor { 
   
   void visitInfinispanConfiguration(InfinispanConfiguration bean);
   
   void visitGlobalConfiguration(GlobalConfiguration bean);
   
   void visitFactoryClassWithPropertiesType(FactoryClassWithPropertiesType bean);
   
   void visitGlobalJmxStatisticsType(GlobalJmxStatisticsType bean);
   
   void visitSerializationType(SerializationType bean);
   
   void visitShutdownType(ShutdownType bean);
   
   void visitTransportType(TransportType bean);
   
   void visitConfiguration(Configuration bean);
   
   void visitAsyncType(AsyncType bean);
   
   void visitBooleanAttributeType(BooleanAttributeType bean);
   
   void visitClusteringType(ClusteringType bean);
   
   void visitCustomInterceptorsType(CustomInterceptorsType bean);
   
   void visitDataContainerType(DataContainerType bean);
   
   void visitDeadlockDetectionType(DeadlockDetectionType bean);
   
   void visitEvictionType(EvictionType bean);
   
   void visitExpirationType(ExpirationType bean);
   
   void visitGroupConfig(GroupsConfiguration bean);
   
   void visitHashType(HashType bean);
   
   void visitL1Type(L1Type bean);
   
   void visitQueryConfigurationBean(QueryConfigurationBean bean);
   
   void visitLockingType(LockingType bean);
      
   void visitStateRetrievalType(StateRetrievalType bean);
   
   void visitSyncType(SyncType bean);
   
   void visitTransactionType(TransactionType bean);
   
   void visitUnsafeType(UnsafeType bean);
   
   void visitCacheLoaderManagerConfig(CacheLoaderManagerConfig bean);
   
   void visitCacheLoaderConfig(CacheLoaderConfig bean);
   
   void visitSingletonStoreConfig(SingletonStoreConfig bean);
   
   void visitAsyncStoreConfig(AsyncStoreConfig bean);

   void visitCustomInterceptorConfig(CustomInterceptorConfig customInterceptorConfig);  
   
   void visitAdvancedExternalizersType(AdvancedExternalizersType bean);
   
   void visitAdvancedExternalizerConfig(AdvancedExternalizerConfig config);

   void visitRecoveryType(Configuration.RecoveryType config);

   void visitStoreAsBinaryType(Configuration.StoreAsBinary config);

   void visitVersioningConfigurationBean(VersioningConfigurationBean config);
}
