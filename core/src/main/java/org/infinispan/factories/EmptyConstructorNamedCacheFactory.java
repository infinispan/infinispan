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
package org.infinispan.factories;


import org.infinispan.batch.BatchContainer;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.config.ConfigurationException;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.eviction.EvictionManager;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.marshall.Marshaller;
import org.infinispan.marshall.VersionAwareMarshaller;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.transaction.TransactionLog;
import org.infinispan.transaction.TransactionTable;

/**
 * Simple factory that just uses reflection and an empty constructor of the component type.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
@DefaultFactoryFor(classes = {CacheNotifier.class, EntryFactory.class, CommandsFactory.class,
                              CacheLoaderManager.class, InvocationContextContainer.class,
                              TransactionTable.class, BatchContainer.class, TransactionLog.class, EvictionManager.class})
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
