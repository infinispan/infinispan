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

import java.util.Arrays;
import java.util.List;

/**
 * DelegatingConfigurationVisitor wraps a list of ConfigurationBeanVisitor visitors and delegates visitor callbacks to
 * all delegates in the list.
 *
 * @author Vladimir Blagojevic
 * @since 5.0
 */
public class DelegatingConfigurationVisitor implements ConfigurationBeanVisitor {

   private List<ConfigurationBeanVisitor> delegates;

   public DelegatingConfigurationVisitor(ConfigurationBeanVisitor[] visitors) {
      delegates = Arrays.asList(visitors);
   }

   public DelegatingConfigurationVisitor(List<ConfigurationBeanVisitor> visitors) {
      delegates = visitors;
   }

   @Override
   public void visitInfinispanConfiguration(InfinispanConfiguration bean) {
      for (ConfigurationBeanVisitor delegate : delegates) {
         delegate.visitInfinispanConfiguration(bean);
      }
   }

   @Override
   public void visitGlobalConfiguration(GlobalConfiguration bean) {
      for (ConfigurationBeanVisitor delegate : delegates) {
         delegate.visitGlobalConfiguration(bean);
      }
   }

   @Override
   public void visitFactoryClassWithPropertiesType(FactoryClassWithPropertiesType bean) {
      for (ConfigurationBeanVisitor delegate : delegates) {
         delegate.visitFactoryClassWithPropertiesType(bean);
      }
   }

   @Override
   public void visitGlobalJmxStatisticsType(GlobalJmxStatisticsType bean) {
      for (ConfigurationBeanVisitor delegate : delegates) {
         delegate.visitGlobalJmxStatisticsType(bean);
      }
   }

   @Override
   public void visitSerializationType(SerializationType bean) {
      for (ConfigurationBeanVisitor delegate : delegates) {
         delegate.visitSerializationType(bean);
      }
   }

   @Override
   public void visitShutdownType(ShutdownType bean) {
      for (ConfigurationBeanVisitor delegate : delegates) {
         delegate.visitShutdownType(bean);
      }
   }

   @Override
   public void visitTransportType(TransportType bean) {
      for (ConfigurationBeanVisitor delegate : delegates) {
         delegate.visitTransportType(bean);
      }
   }

   @Override
   public void visitConfiguration(Configuration bean) {
      for (ConfigurationBeanVisitor delegate : delegates) {
         delegate.visitConfiguration(bean);
      }
   }

   @Override
   public void visitAsyncType(AsyncType bean) {
      for (ConfigurationBeanVisitor delegate : delegates) {
         delegate.visitAsyncType(bean);
      }
   }

   @Override
   public void visitBooleanAttributeType(BooleanAttributeType bean) {
      for (ConfigurationBeanVisitor delegate : delegates) {
         delegate.visitBooleanAttributeType(bean);
      }
   }

   @Override
   public void visitClusteringType(ClusteringType bean) {
      for (ConfigurationBeanVisitor delegate : delegates) {
         delegate.visitClusteringType(bean);
      }
   }

   @Override
   public void visitCustomInterceptorsType(CustomInterceptorsType bean) {
      for (ConfigurationBeanVisitor delegate : delegates) {
         delegate.visitCustomInterceptorsType(bean);
      }
   }

   @Override
   public void visitDataContainerType(DataContainerType bean) {
      for (ConfigurationBeanVisitor delegate : delegates) {
         delegate.visitDataContainerType(bean);
      }
   }

   @Override
   public void visitDeadlockDetectionType(DeadlockDetectionType bean) {
      for (ConfigurationBeanVisitor delegate : delegates) {
         delegate.visitDeadlockDetectionType(bean);
      }
   }

   @Override
   public void visitEvictionType(EvictionType bean) {
      for (ConfigurationBeanVisitor delegate : delegates) {
         delegate.visitEvictionType(bean);
      }
   }

   @Override
   public void visitExpirationType(ExpirationType bean) {
      for (ConfigurationBeanVisitor delegate : delegates) {
         delegate.visitExpirationType(bean);
      }
   }

   @Override
   public void visitGroupConfig(GroupsConfiguration bean) {
      for (ConfigurationBeanVisitor delegate : delegates) {
         delegate.visitGroupConfig(bean);
      }
   }

   @Override
   public void visitHashType(HashType bean) {
      for (ConfigurationBeanVisitor delegate : delegates) {
         delegate.visitHashType(bean);
      }
   }

   @Override
   public void visitL1Type(L1Type bean) {
      for (ConfigurationBeanVisitor delegate : delegates) {
         delegate.visitL1Type(bean);
      }
   }

   @Override
   public void visitQueryConfigurationBean(QueryConfigurationBean bean) {
      for (ConfigurationBeanVisitor delegate : delegates) {
         delegate.visitQueryConfigurationBean(bean);
      }
   }

   @Override
   public void visitLockingType(LockingType bean) {
      for (ConfigurationBeanVisitor delegate : delegates) {
         delegate.visitLockingType(bean);
      }
   }

   @Override
   public void visitStateRetrievalType(StateRetrievalType bean) {
      for (ConfigurationBeanVisitor delegate : delegates) {
         delegate.visitStateRetrievalType(bean);
      }
   }

   @Override
   public void visitSyncType(SyncType bean) {
      for (ConfigurationBeanVisitor delegate : delegates) {
         delegate.visitSyncType(bean);
      }
   }

   @Override
   public void visitTransactionType(TransactionType bean) {
      for (ConfigurationBeanVisitor delegate : delegates) {
         delegate.visitTransactionType(bean);
      }
   }

   @Override
   public void visitUnsafeType(UnsafeType bean) {
      for (ConfigurationBeanVisitor delegate : delegates) {
         delegate.visitUnsafeType(bean);
      }
   }

   @Override
   public void visitCacheLoaderManagerConfig(CacheLoaderManagerConfig bean) {
      for (ConfigurationBeanVisitor delegate : delegates) {
         delegate.visitCacheLoaderManagerConfig(bean);
      }
   }

   @Override
   public void visitCacheLoaderConfig(CacheLoaderConfig bean) {
      for (ConfigurationBeanVisitor delegate : delegates) {
         delegate.visitCacheLoaderConfig(bean);
      }
   }

   @Override
   public void visitSingletonStoreConfig(SingletonStoreConfig bean) {
      for (ConfigurationBeanVisitor delegate : delegates) {
         delegate.visitSingletonStoreConfig(bean);
      }
   }

   @Override
   public void visitAsyncStoreConfig(AsyncStoreConfig bean) {
      for (ConfigurationBeanVisitor delegate : delegates) {
         delegate.visitAsyncStoreConfig(bean);
      }
   }

   @Override
   public void visitCustomInterceptorConfig(CustomInterceptorConfig bean) {
      for (ConfigurationBeanVisitor delegate : delegates) {
         delegate.visitCustomInterceptorConfig(bean);
      }
   }

   @Override
   public void visitAdvancedExternalizersType(AdvancedExternalizersType bean) {
      for (ConfigurationBeanVisitor delegate : delegates) {
         delegate.visitAdvancedExternalizersType(bean);
      }
   }

   @Override
   public void visitAdvancedExternalizerConfig(AdvancedExternalizerConfig bean) {
      for (ConfigurationBeanVisitor delegate : delegates) {
         delegate.visitAdvancedExternalizerConfig(bean);
      }
   }

   @Override
   public void visitRecoveryType(RecoveryType bean) {
      for (ConfigurationBeanVisitor delegate : delegates) {
         delegate.visitRecoveryType(bean);
      }
   }

   @Override
   public void visitStoreAsBinaryType(StoreAsBinary bean) {
      for (ConfigurationBeanVisitor delegate : delegates) {
         delegate.visitStoreAsBinaryType(bean);
      }
   }

   @Override
   public void visitVersioningConfigurationBean(Configuration.VersioningConfigurationBean config) {
      for (ConfigurationBeanVisitor delegate : delegates) {
         delegate.visitVersioningConfigurationBean(config);
      }
   }
}
