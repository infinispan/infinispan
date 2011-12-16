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

import org.infinispan.commands.RemoteCommandsFactory;
import org.infinispan.config.ConfigurationException;
import org.infinispan.distribution.L1Manager;
import org.infinispan.distribution.L1ManagerImpl;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.marshall.jboss.ExternalizerTable;
import org.infinispan.remoting.InboundInvocationHandler;
import org.infinispan.remoting.InboundInvocationHandlerImpl;
import org.infinispan.transaction.xa.TransactionFactory;

/**
 * Factory for building global-scope components which have default empty constructors
 *
 * @author Manik Surtani
 * @author <a href="mailto:galder.zamarreno@jboss.com">Galder Zamarreno</a>
 * @since 4.0
 */
@DefaultFactoryFor(classes = {InboundInvocationHandler.class, RemoteCommandsFactory.class, TransactionFactory.class, L1Manager.class, ExternalizerTable.class })
@Scope(Scopes.GLOBAL)
public class EmptyConstructorFactory extends AbstractComponentFactory implements AutoInstantiableFactory {

   @SuppressWarnings("unchecked")
   public <T> T construct(Class<T> componentType) {
      if (componentType.equals(InboundInvocationHandler.class))
         return (T) new InboundInvocationHandlerImpl();
      else if (componentType.equals(RemoteCommandsFactory.class))
         return (T) new RemoteCommandsFactory();
      else if (componentType.equals(TransactionFactory.class))
         return (T) new TransactionFactory();
      else if (componentType.equals(L1Manager.class))
         return (T) new L1ManagerImpl();
      else if (componentType.equals(ExternalizerTable.class))
         return (T) new ExternalizerTable();

      throw new ConfigurationException("Don't know how to create a " + componentType.getName());
   }
}
