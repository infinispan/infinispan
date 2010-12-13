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

   protected transient Log log = LogFactory.getLog(getClass());

   public void visitInfinispanConfiguration(InfinispanConfiguration bean) {
   }
   
   public void visitAsyncStoreConfig(AsyncStoreConfig bean) {
      defaultVisit(bean);
   }

   public void visitAsyncType(AsyncType bean) {
      defaultVisit(bean);
   }

   public void visitBooleanAttributeType(BooleanAttributeType bean) {
      defaultVisit(bean);

   }

   public void visitCacheLoaderConfig(CacheLoaderConfig bean) {

   }

   public void visitCacheLoaderManagerConfig(CacheLoaderManagerConfig bean) {
      defaultVisit(bean);
   }

   public void visitClusteringType(ClusteringType bean) {
      defaultVisit(bean);
   }

   public void visitConfiguration(Configuration bean) {
      defaultVisit(bean);
   }

   public void visitCustomInterceptorsType(CustomInterceptorsType bean) {
      defaultVisit(bean);
   }

   public void visitDeadlockDetectionType(DeadlockDetectionType bean) {
      defaultVisit(bean);
   }

   public void visitEvictionType(EvictionType bean) {
      defaultVisit(bean);
   }

   public void visitExpirationType(ExpirationType bean) {
      defaultVisit(bean);
   }

   public void visitFactoryClassWithPropertiesType(FactoryClassWithPropertiesType bean) {
      defaultVisit(bean);
   }

   public void visitGlobalConfiguration(GlobalConfiguration bean) {
      defaultVisit(bean);
   }

   public void visitGlobalJmxStatisticsType(GlobalJmxStatisticsType bean) {
      defaultVisit(bean);
   }

   public void visitHashType(HashType bean) {
      defaultVisit(bean);
   }

   public void visitL1Type(L1Type bean) {
      defaultVisit(bean);
   }

   public void visitLockingType(LockingType bean) {
      defaultVisit(bean);
   }
   
   public void visitQueryConfigurationBean(QueryConfigurationBean bean) {
      defaultVisit(bean);
   }


   public void visitSerializationType(SerializationType bean) {
      defaultVisit(bean);
   }

   public void visitShutdownType(ShutdownType bean) {
      defaultVisit(bean);
   }

   public void visitSingletonStoreConfig(SingletonStoreConfig bean) {
      defaultVisit(bean);
   }

   public void visitStateRetrievalType(StateRetrievalType bean) {
      defaultVisit(bean);
   }

   public void visitSyncType(SyncType bean) {
      defaultVisit(bean);
   }

   public void visitTransactionType(TransactionType bean) {
      defaultVisit(bean);
   }

   public void visitTransportType(TransportType bean) {
      defaultVisit(bean);
   }

   public void visitUnsafeType(UnsafeType bean) {
      defaultVisit(bean);
   }
   
   public void visitCustomInterceptorConfig(CustomInterceptorConfig bean) {
      defaultVisit(bean);
   }
   
   public void visitExternalizerConfig(ExternalizerConfig bean) {
      defaultVisit(bean);
   }
   
   public void visitExternalizersType(GlobalConfiguration.ExternalizersType bean) {
      defaultVisit(bean);
   }

   public void defaultVisit(AbstractConfigurationBean c) {
   }

}
