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

import org.infinispan.config.ConfigurationException;
import org.infinispan.container.DataContainer;
import org.infinispan.container.DefaultDataContainer;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionThreadPolicy;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.util.Comparing;

/**
 * Constructs the data container
 * 
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Vladimir Blagojevic
 * @since 4.0
 */
@DefaultFactoryFor(classes = DataContainer.class)
public class DataContainerFactory extends AbstractNamedCacheComponentFactory implements
         AutoInstantiableFactory {

   @Override
   @SuppressWarnings("unchecked")
   public <T> T construct(Class<T> componentType) {
      if (configuration.dataContainer().dataContainer() != null) {
         return (T) configuration.dataContainer().dataContainer();
      } else {
         EvictionStrategy st = configuration.eviction().strategy();
         int level = configuration.locking().concurrencyLevel();
         Comparing comparingKey = configuration.dataContainer().comparingKey();
         Comparing comparingValue = configuration.dataContainer().comparingValue();

         switch (st) {
            case NONE:         
               return (T) DefaultDataContainer.unBoundedDataContainer(
                     level, comparingKey, comparingValue);
            case UNORDERED:   
            case LRU:
            case FIFO:
            case LIRS:
               int maxEntries = configuration.eviction().maxEntries();
               //handle case when < 0 value signifies unbounded container 
               if(maxEntries < 0) {
                   return (T) DefaultDataContainer.unBoundedDataContainer(
                         level, comparingKey, comparingValue);
               }
               EvictionThreadPolicy policy = configuration.eviction().threadPolicy();
               return (T) DefaultDataContainer.boundedDataContainer(
                     level, maxEntries, st, policy, comparingKey, comparingValue);
            default:
               throw new ConfigurationException("Unknown eviction strategy "
                        + configuration.eviction().strategy());
         }
      }
   }
}
