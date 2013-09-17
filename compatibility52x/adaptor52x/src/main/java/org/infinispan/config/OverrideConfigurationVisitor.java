package org.infinispan.config;

import org.infinispan.CacheException;
import org.infinispan.commons.util.ReflectionUtil;
import org.infinispan.config.Configuration.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * OverrideConfigurationVisitor breaks down fields of Configuration object to individual components
 * and then compares them for field updates.
 * 
 * @author Vladimir Blagojevic
 * @since 4.0
 */
public class OverrideConfigurationVisitor extends AbstractConfigurationBeanVisitor {

   private AsyncType asyncType = null;
   private CacheLoaderManagerConfig cacheLoaderManagerConfig = null;
   private ClusteringType clusteringType = null;
   private final Map <String,BooleanAttributeType> bats = new HashMap<String,BooleanAttributeType>();

   private CustomInterceptorsType customInterceptorsType = null;
   private DeadlockDetectionType deadlockDetectionType = null;
   private EvictionType evictionType = null;
   private ExpirationType expirationType = null;
   private GroupsConfiguration groupsConfiguration = null;
   private HashType hashType = null;
   private L1Type l1Type = null;
   private LockingType lockingType = null;
   private StateRetrievalType stateRetrievalType = null;
   private SyncType syncType = null;
   private TransactionType transactionType = null;
   private UnsafeType unsafeType = null;
   private QueryConfigurationBean indexingType = null;
   private RecoveryType recoveryType = null;
   private StoreAsBinary storeAsBinary = null;
   private DataContainerType dataContainerType;
   private VersioningConfigurationBean versioningType;

   public void override(OverrideConfigurationVisitor override) {
      
      // special handling for BooleanAttributeType
      Set<Entry<String, BooleanAttributeType>> entrySet = override.bats.entrySet();
      for (Entry<String, BooleanAttributeType> entry : entrySet) {
         String booleanAttributeName = entry.getKey();
         BooleanAttributeType attributeType = bats.get(booleanAttributeName);
         BooleanAttributeType overrideAttributeType = override.bats.get(booleanAttributeName);
         overrideFields(attributeType, overrideAttributeType);
      }
      
      //do we need to make clones of complex objects like list of cache loaders?
      overrideFields(cacheLoaderManagerConfig, override.cacheLoaderManagerConfig);      
      
      //everything else...
      overrideFields(asyncType, override.asyncType);
      overrideFields(clusteringType, override.clusteringType);
      overrideFields(deadlockDetectionType, override.deadlockDetectionType);
      overrideFields(evictionType, override.evictionType);
      overrideFields(expirationType, override.expirationType);
      overrideFields(groupsConfiguration, override.groupsConfiguration);
      overrideFields(hashType, override.hashType);
      overrideFields(l1Type, override.l1Type);
      overrideFields(lockingType, override.lockingType);
      overrideFields(stateRetrievalType, override.stateRetrievalType);
      overrideFields(syncType, override.syncType);
      overrideFields(transactionType, override.transactionType);
      overrideFields(recoveryType, override.recoveryType);
      overrideFields(unsafeType, override.unsafeType);
      overrideFields(indexingType, override.indexingType);
      overrideFields(customInterceptorsType, override.customInterceptorsType);
      overrideFields(storeAsBinary, override.storeAsBinary);
      overrideFields(dataContainerType, override.dataContainerType);
      overrideFields(versioningType, override.versioningType);
   }

   private void overrideFields(AbstractConfigurationBean bean, AbstractConfigurationBean overrides) {
      if (overrides != null && bean != null) {
         // does this component have overridden fields?
         for (String overridenField : overrides.overriddenConfigurationElements) {
            try {
               // If the original configuration option has overriden fields,
               // when overriding maintain the overriden state
               bean.overriddenConfigurationElements.add(overridenField);
               ReflectionUtil.setValue(bean, overridenField, ReflectionUtil.getValue(overrides, overridenField));
            } catch (Exception e1) {
               throw new CacheException("Could not apply value for field " + overridenField
                        + " from instance " + overrides + " on instance " + this, e1);
            }
         }
      } 
   }

   @Override
   public void visitAsyncType(AsyncType bean) {
      asyncType = bean;
   }

   @Override
   public void visitBooleanAttributeType(BooleanAttributeType bat) {
      bats.put(bat.getFieldName(), bat);
   }

   @Override
   public void visitCacheLoaderManagerConfig(CacheLoaderManagerConfig bean) {
      cacheLoaderManagerConfig = bean;
   }

   @Override
   public void visitClusteringType(ClusteringType bean) {
      clusteringType = bean;
   }

   @Override
   public void visitCustomInterceptorsType(CustomInterceptorsType bean) {
      customInterceptorsType = bean;
   }

   @Override
   public void visitDeadlockDetectionType(DeadlockDetectionType bean) {
      deadlockDetectionType = bean;
   }

   @Override
   public void visitEvictionType(EvictionType bean) {
      evictionType = bean;
   }

   @Override
   public void visitExpirationType(ExpirationType bean) {
      expirationType = bean;
   }
   
   @Override
   public void visitGroupConfig(GroupsConfiguration bean) {
      groupsConfiguration = bean;
   }

   @Override
   public void visitHashType(HashType bean) {
      hashType = bean;
   }

   @Override
   public void visitL1Type(L1Type bean) {
      l1Type = bean;
   }

   @Override
   public void visitLockingType(LockingType bean) {
      lockingType = bean;
   }

   @Override
   public void visitStateRetrievalType(StateRetrievalType bean) {
      stateRetrievalType = bean;
   }

   @Override
   public void visitSyncType(SyncType bean) {
      syncType = bean;
   }

   @Override
   public void visitTransactionType(TransactionType bean) {
      transactionType = bean;
   }

   @Override
   public void visitUnsafeType(UnsafeType bean) {
      unsafeType = bean;
   }

   @Override
   public void visitQueryConfigurationBean(QueryConfigurationBean bean) {
      indexingType = bean;
   }

   @Override
   public void visitRecoveryType(RecoveryType config) {
      this.recoveryType = config;
   }

   @Override
   public void visitStoreAsBinaryType(StoreAsBinary config) {
      this.storeAsBinary = config;
   }

   @Override
   public void visitDataContainerType(DataContainerType bean) {
      this.dataContainerType = bean;
   }

   @Override
   public void visitVersioningConfigurationBean(VersioningConfigurationBean config) {
      this.versioningType = config;
   }

}
