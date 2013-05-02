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

import org.infinispan.commands.CancellationService;
import org.infinispan.commands.CancellationServiceImpl;
import org.infinispan.commands.RemoteCommandsFactory;
import org.infinispan.config.ConfigurationException;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.marshall.jboss.ExternalizerTable;
import org.infinispan.remoting.InboundInvocationHandler;
import org.infinispan.remoting.InboundInvocationHandlerImpl;
import org.infinispan.topology.ClusterTopologyManager;
import org.infinispan.topology.ClusterTopologyManagerImpl;
import org.infinispan.topology.DefaultRebalancePolicy;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.topology.LocalTopologyManagerImpl;
import org.infinispan.topology.RebalancePolicy;
import org.infinispan.util.DefaultTimeService;
import org.infinispan.util.TimeService;
import org.infinispan.xsite.BackupReceiverRepository;
import org.infinispan.xsite.BackupReceiverRepositoryImpl;

/**
 * Factory for building global-scope components which have default empty constructors
 *
 * @author Manik Surtani
 * @author <a href="mailto:galder.zamarreno@jboss.com">Galder Zamarreno</a>
 * @since 4.0
 */

@DefaultFactoryFor(classes = {InboundInvocationHandler.class, RemoteCommandsFactory.class, ExternalizerTable.class,
                              RebalancePolicy.class, BackupReceiverRepository.class, CancellationService.class,
                              TimeService.class})
@Scope(Scopes.GLOBAL)
public class EmptyConstructorFactory extends AbstractComponentFactory implements AutoInstantiableFactory {

   @Override
   @SuppressWarnings("unchecked")
   public <T> T construct(Class<T> componentType) {
      if (componentType.equals(InboundInvocationHandler.class))
         return (T) new InboundInvocationHandlerImpl();
      else if (componentType.equals(RemoteCommandsFactory.class))
         return (T) new RemoteCommandsFactory();
      else if (componentType.equals(ExternalizerTable.class))
         return (T) new ExternalizerTable();
      else if (componentType.equals(LocalTopologyManager.class))
         return (T) new LocalTopologyManagerImpl();
      else if (componentType.equals(ClusterTopologyManager.class))
         return (T) new ClusterTopologyManagerImpl();
      else if (componentType.equals(RebalancePolicy.class))
         return (T) new DefaultRebalancePolicy();
      else if (componentType.equals(BackupReceiverRepository.class))
         return (T) new BackupReceiverRepositoryImpl();
      else if (componentType.equals(CancellationService.class))
         return (T) new CancellationServiceImpl();
      else if (componentType.equals(TimeService.class)) {
         return (T) new DefaultTimeService();
      }

      throw new ConfigurationException("Don't know how to create a " + componentType.getName());
   }
}
