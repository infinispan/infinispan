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
            attributeType.enabled = entry.getValue().enabled;
         }
      }
      
      //do we need to make clones of complex objects like list of cache loaders?
      overrideFields(cacheLoaderManagerConfig, override.cacheLoaderManagerConfig);      
      
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
