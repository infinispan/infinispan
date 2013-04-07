/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

package org.infinispan.factories;

import org.infinispan.container.EntryFactory;
import org.infinispan.container.EntryFactoryImpl;
import org.infinispan.container.IncrementalVersionableEntryFactoryImpl;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.InternalEntryFactoryImpl;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.concurrent.IsolationLevel;

@DefaultFactoryFor(classes = {EntryFactory.class, InternalEntryFactory.class})
public class EntryMetaFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {

   @Override
   @SuppressWarnings("unchecked")
   public <T> T construct(Class<T> componentType) {

      // If we are repeatable-read and have write skew checking enabled and are clustered, lets create an appropriate EntryFactory.
      boolean useVersioning = configuration.clustering().cacheMode().isClustered() &&
            configuration.transaction().transactionMode().isTransactional() &&
            configuration.locking().isolationLevel() == IsolationLevel.REPEATABLE_READ &&
            configuration.locking().writeSkewCheck() &&
            configuration.transaction().lockingMode() == LockingMode.OPTIMISTIC;

      if (componentType.equals(EntryFactory.class)) {
         if (useVersioning)
            return (T) new IncrementalVersionableEntryFactoryImpl();
         else
            return (T) new EntryFactoryImpl();
      } else {
         return (T) new InternalEntryFactoryImpl();
      }
   }
}
