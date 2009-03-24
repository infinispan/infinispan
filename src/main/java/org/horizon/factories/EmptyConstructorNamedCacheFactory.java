/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2000 - 2008, Red Hat Middleware LLC, and individual contributors
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
package org.horizon.factories;


import org.horizon.batch.BatchContainer;
import org.horizon.commands.CommandsFactory;
import org.horizon.config.ConfigurationException;
import org.horizon.factories.annotations.DefaultFactoryFor;
import org.horizon.factories.context.ContextFactory;
import org.horizon.invocation.InvocationContextContainer;
import org.horizon.loader.CacheLoaderManager;
import org.horizon.marshall.Marshaller;
import org.horizon.marshall.VersionAwareMarshaller;
import org.horizon.notifications.cachelistener.CacheNotifier;
import org.horizon.transaction.TransactionTable;
import org.horizon.transaction.TransactionLog;
import org.horizon.eviction.EvictionManager;

/**
 * Simple factory that just uses reflection and an empty constructor of the component type.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
@DefaultFactoryFor(classes = {CacheNotifier.class, EntryFactory.class, CommandsFactory.class,
                              CacheLoaderManager.class, InvocationContextContainer.class,
                              TransactionTable.class, BatchContainer.class, ContextFactory.class,
                              TransactionLog.class, EvictionManager.class})
public class EmptyConstructorNamedCacheFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {
   @Override
   public <T> T construct(Class<T> componentType) {
      try {
         if (componentType.isInterface()) {
            Class componentImpl;
            if (componentType.equals(Marshaller.class)) {
               componentImpl = VersionAwareMarshaller.class;
            } else {
               // add an "Impl" to the end of the class name and try again
               componentImpl = getClass().getClassLoader().loadClass(componentType.getName() + "Impl");
            }
            return componentType.cast(componentImpl.newInstance());
         } else {
            return componentType.newInstance();
         }
      }
      catch (Exception e) {
         throw new ConfigurationException("Unable to create component " + componentType, e);
      }
   }
}
