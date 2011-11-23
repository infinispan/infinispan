/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
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
package org.infinispan.factories;


import static org.infinispan.commons.util.Util.getInstance;
import static org.infinispan.commons.util.Util.loadClass;

import org.infinispan.api.marshall.StreamingMarshaller;
import org.infinispan.batch.BatchContainer;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.container.EntryFactory;
import org.infinispan.container.EntryFactoryImpl;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.context.NonTransactionalInvocationContextContainer;
import org.infinispan.context.TransactionalInvocationContextContainer;
import org.infinispan.eviction.EvictionManager;
import org.infinispan.eviction.PassivationManager;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.marshall.VersionAwareMarshaller;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.statetransfer.StateTransferLock;
import org.infinispan.transaction.TransactionCoordinator;
import org.infinispan.transaction.TransactionLog;
import org.infinispan.transaction.xa.recovery.RecoveryAdminOperations;

/**
 * Simple factory that just uses reflection and an empty constructor of the component type.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
@DefaultFactoryFor(classes = {CacheNotifier.class, EntryFactory.class, CommandsFactory.class,
        CacheLoaderManager.class, InvocationContextContainer.class, PassivationManager.class,
        BatchContainer.class, TransactionLog.class, EvictionManager.class, InvocationContextContainer.class,
        TransactionCoordinator.class, RecoveryAdminOperations.class, StateTransferLock.class, ClusteringDependentLogic.class})
public class EmptyConstructorNamedCacheFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {

   @Override
   @SuppressWarnings("unchecked")
   public <T> T construct(Class<T> componentType) {
      if (componentType.isInterface()) {
         Class componentImpl;
         if (componentType.equals(ClusteringDependentLogic.class)) {
            if (configuration.getCacheMode().isReplicated() || !configuration.getCacheMode().isClustered() || configuration.getCacheMode().isInvalidation()) {
               return componentType.cast(new ClusteringDependentLogic.AllNodesLogic());
            } else {
               return componentType.cast(new ClusteringDependentLogic.DistributionLogic());
            }
         } else if (componentType.equals(StreamingMarshaller.class)) {
            VersionAwareMarshaller versionAwareMarshaller = getInstance(VersionAwareMarshaller.class);
            return componentType.cast(versionAwareMarshaller);
         } else if (componentType.equals(EntryFactory.class)) {
            return componentType.cast(getInstance(EntryFactoryImpl.class));
         } else if(componentType.equals(InvocationContextContainer.class)) {
            componentImpl = configuration.isTransactionalCache() ? TransactionalInvocationContextContainer.class
                  : NonTransactionalInvocationContextContainer.class;
            return componentType.cast(getInstance(componentImpl));
         }else {
            // add an "Impl" to the end of the class name and try again
            componentImpl = loadClass(componentType.getName() + "Impl", configuration.getClassLoader());
         }
         return componentType.cast(getInstance(componentImpl));
      } else {
         return getInstance(componentType);
      }
   }
}
