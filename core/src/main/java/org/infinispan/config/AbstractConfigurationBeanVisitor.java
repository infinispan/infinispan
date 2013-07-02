package org.infinispan.config;

import org.infinispan.config.Configuration.AsyncType;
import org.infinispan.config.Configuration.BooleanAttributeType;
import org.infinispan.config.Configuration.ClusteringType;
import org.infinispan.config.Configuration.CustomInterceptorsType;
import org.infinispan.config.Configuration.DataContainerType;
import org.infinispan.config.Configuration.DeadlockDetectionType;
import org.infinispan.config.Configuration.EvictionType;
import org.infinispan.config.Configuration.ExpirationType;
import org.infinispan.config.Configuration.HashType;
import org.infinispan.config.Configuration.L1Type;
import org.infinispan.config.Configuration.LockingType;
import org.infinispan.config.Configuration.QueryConfigurationBean;
import org.infinispan.config.Configuration.StateRetrievalType;
import org.infinispan.config.Configuration.SyncType;
import org.infinispan.config.Configuration.TransactionType;
import org.infinispan.config.Configuration.UnsafeType;
import org.infinispan.config.GlobalConfiguration.FactoryClassWithPropertiesType;
import org.infinispan.config.GlobalConfiguration.GlobalJmxStatisticsType;
import org.infinispan.config.GlobalConfiguration.SerializationType;
import org.infinispan.config.GlobalConfiguration.ShutdownType;
import org.infinispan.config.GlobalConfiguration.TransportType;
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.loaders.decorators.AsyncStoreConfig;
import org.infinispan.loaders.decorators.SingletonStoreConfig;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * AbstractConfigurationBeanVisitor is a convenience super class for ConfigurationBeanVisitor
 * classes.
 *
 * <p>
 *
 * Subclasses of AbstractConfigurationBeanVisitor should define the most parameter type specific
 * definitions of <code>void visit(AbstractConfigurationBean bean); </code> method. These methods
 * are going to be invoked by traverser as it comes across these types during traversal of
 * <code>InfinispanConfiguration</code> tree.
 *
 * <p>
 *
 * For example, method <code>public void visit(SingletonStoreConfig ssc)</code> defined in a
 * subclass of this class is going to be invoked as the traverser comes across instance(s) of
 * SingletonStoreConfig.
 *
 * @author Vladimir Blagojevic
 * @since 4.0
 */
public abstract class AbstractConfigurationBeanVisitor implements ConfigurationBeanVisitor {

   private static final Log log = LogFactory.getLog(AbstractConfigurationBeanVisitor.class);

   @Override
   public void visitInfinispanConfiguration(InfinispanConfiguration bean) {
   }

   @Override
   public void visitAsyncStoreConfig(AsyncStoreConfig bean) {
      defaultVisit(bean);
   }

   @Override
   public void visitAsyncType(AsyncType bean) {
      defaultVisit(bean);
   }

   @Override
   public void visitBooleanAttributeType(BooleanAttributeType bean) {
      defaultVisit(bean);
   }

   @Override
   public void visitCacheLoaderConfig(CacheLoaderConfig bean) {
      if(bean instanceof AbstractConfigurationBean)
         defaultVisit((AbstractConfigurationBean) bean);
   }

   @Override
   public void visitCacheLoaderManagerConfig(CacheLoaderManagerConfig bean) {
      defaultVisit(bean);
   }

   @Override
   public void visitClusteringType(ClusteringType bean) {
      defaultVisit(bean);
   }

   @Override
   public void visitConfiguration(Configuration bean) {
      defaultVisit(bean);
   }

   @Override
   public void visitCustomInterceptorsType(CustomInterceptorsType bean) {
      defaultVisit(bean);
   }

   @Override
   public void visitDataContainerType(DataContainerType bean) {
      defaultVisit(bean);
   }

   @Override
   public void visitDeadlockDetectionType(DeadlockDetectionType bean) {
      defaultVisit(bean);
   }

   @Override
   public void visitEvictionType(EvictionType bean) {
      defaultVisit(bean);
   }

   @Override
   public void visitExpirationType(ExpirationType bean) {
      defaultVisit(bean);
   }

   @Override
   public void visitFactoryClassWithPropertiesType(FactoryClassWithPropertiesType bean) {
      defaultVisit(bean);
   }

   @Override
   public void visitGlobalConfiguration(GlobalConfiguration bean) {
      defaultVisit(bean);
   }

   @Override
   public void visitGlobalJmxStatisticsType(GlobalJmxStatisticsType bean) {
      defaultVisit(bean);
   }

   @Override
   public void visitGroupConfig(GroupsConfiguration bean) {
      defaultVisit(bean);
   }

   @Override
   public void visitHashType(HashType bean) {
      defaultVisit(bean);
   }

   @Override
   public void visitL1Type(L1Type bean) {
      defaultVisit(bean);
   }

   @Override
   public void visitLockingType(LockingType bean) {
      defaultVisit(bean);
   }

   @Override
   public void visitQueryConfigurationBean(QueryConfigurationBean bean) {
      defaultVisit(bean);
   }

   @Override
   public void visitSerializationType(SerializationType bean) {
      defaultVisit(bean);
   }

   @Override
   public void visitShutdownType(ShutdownType bean) {
      defaultVisit(bean);
   }

   @Override
   public void visitSingletonStoreConfig(SingletonStoreConfig bean) {
      defaultVisit(bean);
   }

   @Override
   public void visitStateRetrievalType(StateRetrievalType bean) {
      defaultVisit(bean);
   }

   @Override
   public void visitSyncType(SyncType bean) {
      defaultVisit(bean);
   }

   @Override
   public void visitTransactionType(TransactionType bean) {
      defaultVisit(bean);
   }

   @Override
   public void visitTransportType(TransportType bean) {
      defaultVisit(bean);
   }

   @Override
   public void visitUnsafeType(UnsafeType bean) {
      defaultVisit(bean);
   }

   @Override
   public void visitCustomInterceptorConfig(CustomInterceptorConfig bean) {
      defaultVisit(bean);
   }

   @Override
   public void visitAdvancedExternalizerConfig(AdvancedExternalizerConfig bean) {
      defaultVisit(bean);
   }

   @Override
   public void visitAdvancedExternalizersType(GlobalConfiguration.AdvancedExternalizersType bean) {
      defaultVisit(bean);
   }

   @Override
   public void visitRecoveryType(Configuration.RecoveryType config) {
      defaultVisit(config);
   }

   @Override
   public void visitStoreAsBinaryType(Configuration.StoreAsBinary config) {
      defaultVisit(config);
   }

   @Override
   public void visitVersioningConfigurationBean(Configuration.VersioningConfigurationBean config) {
      defaultVisit(config);
   }

   public void defaultVisit(AbstractConfigurationBean c) {
   }
}
