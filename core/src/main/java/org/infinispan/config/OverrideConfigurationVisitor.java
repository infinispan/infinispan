/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
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
package org.infinispan.config;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.infinispan.CacheException;
import org.infinispan.config.Configuration.AsyncType;
import org.infinispan.config.Configuration.BooleanAttributeType;
import org.infinispan.config.Configuration.ClusteringType;
import org.infinispan.config.Configuration.CustomInterceptorsType;
import org.infinispan.config.Configuration.DeadlockDetectionType;
import org.infinispan.config.Configuration.EvictionType;
import org.infinispan.config.Configuration.ExpirationType;
import org.infinispan.config.Configuration.HashType;
import org.infinispan.config.Configuration.L1Type;
import org.infinispan.config.Configuration.LockingType;
import org.infinispan.config.Configuration.StateRetrievalType;
import org.infinispan.config.Configuration.SyncType;
import org.infinispan.config.Configuration.TransactionType;
import org.infinispan.config.Configuration.UnsafeType;
import org.infinispan.loaders.AbstractCacheStoreConfig;
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.util.ReflectionUtil;

/**
 * OverrideConfigurationVisitor breaks down fields of Configuration object to individual components
 * and then compares them for field updates.
 * 
 * @author Vladimir Blagojevic
 * @since 4.0
 */
public class OverrideConfigurationVisitor extends AbstractConfigurationBeanVisitor {

   AsyncType asyncType = null;
   CacheLoaderConfig cacheLoaderConfig = null;
   CacheLoaderManagerConfig cacheLoaderManagerConfig = null;
   ClusteringType clusteringType = null;
   Map <String,BooleanAttributeType> bats = new HashMap<String,BooleanAttributeType>();

   CustomInterceptorsType customInterceptorsType = null;
   DeadlockDetectionType deadlockDetectionType = null;
   EvictionType evictionType = null;
   ExpirationType expirationType = null;
   HashType hashType = null;
   L1Type l1Type = null;
   LockingType lockingType = null;
   StateRetrievalType stateRetrievalType = null;
   SyncType syncType = null;
   TransactionType transactionType = null;
   UnsafeType unsafeType = null;

   public void override(OverrideConfigurationVisitor override) {
      
      //special handling for BooleanAttributeType
      Set<Entry<String, BooleanAttributeType>> entrySet = override.bats.entrySet();      
      for (Entry<String, BooleanAttributeType> entry : entrySet) {
         BooleanAttributeType attributeType = bats.get(entry.getKey());
         if(attributeType != null) {
            attributeType.setEnabled(entry.getValue().enabled);
         }
      }
      
      //special handling for cache loader manager
      overrideFields(cacheLoaderManagerConfig, override.cacheLoaderManagerConfig);
      CacheLoaderManagerConfig config = override.cacheLoaderManagerConfig;
      List<CacheLoaderConfig> cll2 = config.getCacheLoaderConfigs();
      List<CacheLoaderConfig> cll1 = cacheLoaderManagerConfig.getCacheLoaderConfigs();

      if (cll1.isEmpty() && !cll2.isEmpty()) {
         cll1.addAll(cll2);
      } else if (!cll1.isEmpty() && !cll2.isEmpty()) {
         Iterator<CacheLoaderConfig> i1 = cll1.iterator();
         Iterator<CacheLoaderConfig> i2 = cll2.iterator();
         for (; i1.hasNext() && i2.hasNext();) {
            CacheLoaderConfig l1 = i1.next();
            CacheLoaderConfig l2 = i2.next();
            if (l1.getCacheLoaderClassName().equals(l2.getCacheLoaderClassName())) {
               overrideFields((AbstractConfigurationBean) l1, (AbstractConfigurationBean) l2);
               if(l1 instanceof AbstractCacheStoreConfig && l2 instanceof AbstractCacheStoreConfig) {
                  overrideFields(((AbstractCacheStoreConfig) l1).getSingletonStoreConfig(),
                           ((AbstractCacheStoreConfig) l2).getSingletonStoreConfig());
                  overrideFields(((AbstractCacheStoreConfig) l1).getAsyncStoreConfig(),
                           ((AbstractCacheStoreConfig) l2).getAsyncStoreConfig());
               }
            }
         }
         while (i2.hasNext()) {
            cll1.add(i2.next());
         }
      }
      
      //everything else...
      overrideFields(asyncType, override.asyncType);
      overrideFields(clusteringType, override.clusteringType);
      overrideFields(deadlockDetectionType, override.deadlockDetectionType);
      overrideFields(evictionType, override.evictionType);
      overrideFields(expirationType, override.expirationType);
      overrideFields(hashType, override.hashType);
      overrideFields(l1Type, override.l1Type);
      overrideFields(lockingType, override.lockingType);
      overrideFields(stateRetrievalType, override.stateRetrievalType);
      overrideFields(syncType, override.syncType);
      overrideFields(transactionType, override.transactionType);
      overrideFields(unsafeType, override.unsafeType);
   }

   private void overrideFields(AbstractConfigurationBean bean, AbstractConfigurationBean overrides) {
      if (overrides != null && bean != null) {
         // does this component have overridden fields?
         for (String overridenField : overrides.overriddenConfigurationElements) {
            try {
               ReflectionUtil.setValue(bean, overridenField, ReflectionUtil.getValue(overrides,overridenField));
            } catch (Exception e1) {
               throw new CacheException("Could not apply value for field " + overridenField
                        + " from instance " + overrides + " on instance " + this, e1);
            }
         }
      } 
   }

   public void visitAsyncType(AsyncType bean) {
      asyncType = bean;
   }
   
   public void visitBooleanAttributeType(BooleanAttributeType bat) {
      bats.put(bat.getFieldName(), bat);
   }

   public void visitCacheLoaderManagerConfig(CacheLoaderManagerConfig bean) {
      cacheLoaderManagerConfig = bean;
   }

   public void visitClusteringType(ClusteringType bean) {
      clusteringType = bean;
   }

   public void visitCustomInterceptorsType(CustomInterceptorsType bean) {
      customInterceptorsType = bean;
   }

   public void visitDeadlockDetectionType(DeadlockDetectionType bean) {
      deadlockDetectionType = bean;
   }

   public void visitEvictionType(EvictionType bean) {
      evictionType = bean;
   }

   public void visitExpirationType(ExpirationType bean) {
      expirationType = bean;
   }

   public void visitHashType(HashType bean) {
      hashType = bean;
   }

   public void visitL1Type(L1Type bean) {
      l1Type = bean;
   }

   public void visitLockingType(LockingType bean) {
      lockingType = bean;
   }

   public void visitStateRetrievalType(StateRetrievalType bean) {
      stateRetrievalType = bean;
   }

   public void visitSyncType(SyncType bean) {
      syncType = bean;
   }

   public void visitTransactionType(TransactionType bean) {
      transactionType = bean;
   }

   public void visitUnsafeType(UnsafeType bean) {
      unsafeType = bean;
   }
}
