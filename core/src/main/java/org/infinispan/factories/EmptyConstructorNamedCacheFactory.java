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


import org.infinispan.batch.BatchContainer;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.CommandsFactoryImpl;
import org.infinispan.config.ConfigurationException;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.context.NonTransactionalInvocationContextContainer;
import org.infinispan.context.TransactionalInvocationContextContainer;
import org.infinispan.eviction.EvictionManager;
import org.infinispan.eviction.EvictionManagerImpl;
import org.infinispan.eviction.PassivationManager;
import org.infinispan.eviction.PassivationManagerImpl;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.CacheLoaderManagerImpl;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.notifications.cachelistener.CacheNotifierImpl;
import org.infinispan.statetransfer.StateTransferLock;
import org.infinispan.statetransfer.StateTransferLockImpl;
import org.infinispan.totalorder.TotalOrderValidator;
import org.infinispan.transaction.TransactionCoordinator;
import org.infinispan.transaction.xa.recovery.RecoveryAdminOperations;

import static org.infinispan.util.Util.getInstance;

/**
 * Simple factory that just uses reflection and an empty constructor of the component type.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
@DefaultFactoryFor(classes = {CacheNotifier.class, CommandsFactory.class,
        CacheLoaderManager.class, InvocationContextContainer.class, PassivationManager.class,
        BatchContainer.class, EvictionManager.class,
        TransactionCoordinator.class, RecoveryAdminOperations.class, StateTransferLock.class,
        ClusteringDependentLogic.class, TotalOrderValidator.class})
public class EmptyConstructorNamedCacheFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {

    @Override
    @SuppressWarnings("unchecked")
    public <T> T construct(Class<T> componentType) {
        Class componentImpl;
        if (componentType.equals(ClusteringDependentLogic.class)) {
            if (configuration.getCacheMode().isReplicated() || !configuration.getCacheMode().isClustered() || configuration.getCacheMode().isInvalidation()) {
                return componentType.cast(new ClusteringDependentLogic.AllNodesLogic());
            } else {
                return componentType.cast(new ClusteringDependentLogic.DistributionLogic());
            }
        } else if (componentType.equals(InvocationContextContainer.class)) {
            componentImpl = configuration.isTransactionalCache() ? TransactionalInvocationContextContainer.class
                    : NonTransactionalInvocationContextContainer.class;
            return componentType.cast(getInstance(componentImpl));
        } else if (componentType.equals(CacheNotifier.class)) {
            return (T) new CacheNotifierImpl();
        } else if (componentType.equals(CommandsFactory.class)) {
            return (T) new CommandsFactoryImpl();
        } else if (componentType.equals(CacheLoaderManager.class)) {
            return (T) new CacheLoaderManagerImpl();
        } else if (componentType.equals(PassivationManager.class)) {
            return (T) new PassivationManagerImpl();
        } else if (componentType.equals(BatchContainer.class)) {
            return (T) new BatchContainer();
        } else if (componentType.equals(TransactionCoordinator.class)) {
            return (T) new TransactionCoordinator();
        } else if (componentType.equals(RecoveryAdminOperations.class)) {
            return (T) new RecoveryAdminOperations();
        } else if (componentType.equals(StateTransferLock.class)) {
            return (T) new StateTransferLockImpl();
        } else if (componentType.equals(EvictionManager.class)) {
            return (T) new EvictionManagerImpl();
            //Pedro -- added the constructor for the total order validator
        } else if (componentType.equals(TotalOrderValidator.class)) {
            return (T) new TotalOrderValidator();
        }


        throw new ConfigurationException("Don't know how to create a " + componentType.getName());

    }
}
