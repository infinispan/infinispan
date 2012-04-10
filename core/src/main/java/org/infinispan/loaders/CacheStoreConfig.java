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
package org.infinispan.loaders;

import org.infinispan.loaders.decorators.AsyncStoreConfig;
import org.infinispan.loaders.decorators.SingletonStoreConfig;

/**
 * Defines config elements for all CacheStoreConfigs.
 *
 * @author Mircea.Markus@jboss.com
 * @author Vladimir Blagojevic
 */
public interface CacheStoreConfig extends CacheLoaderConfig, Cloneable {

   Boolean isPurgeOnStartup();

   Boolean isFetchPersistentState();

   /**
    * @deprecated use {@link #fetchPersistentState(Boolean)} instead
    */
   @Deprecated
   void setFetchPersistentState(Boolean fetchPersistentState);

   /**
    * @deprecated use {@link #ignoreModifications(Boolean)} instead
    */
   @Deprecated
   void setIgnoreModifications(Boolean ignoreModifications);
   
   CacheStoreConfig fetchPersistentState(Boolean fetchPersistentState);

   CacheStoreConfig ignoreModifications(Boolean ignoreModifications);

   Boolean isIgnoreModifications();

   /**
    * @deprecated use {@link #purgeOnStartup(Boolean)} instead
    */
   @Deprecated
   void setPurgeOnStartup(Boolean purgeOnStartup);
   
   CacheStoreConfig purgeOnStartup(Boolean purgeOnStartup);

   SingletonStoreConfig getSingletonStoreConfig();

   /**
    * @deprecated use {@link #singletonStore()} instead
    */
   @Deprecated
   void setSingletonStoreConfig(SingletonStoreConfig singletonStoreConfig);
   
   AsyncStoreConfig getAsyncStoreConfig();

   /**
    * @deprecated use {@link #asyncStore()} instead
    */
   @Deprecated
   void setAsyncStoreConfig(AsyncStoreConfig asyncStoreConfig);
   
   Boolean isPurgeSynchronously();

   /**
    * @deprecated use {@link #purgeSynchronously(Boolean)} instead
    */
   @Deprecated
   void setPurgeSynchronously(Boolean purgeSynchronously);
   
   CacheStoreConfig purgeSynchronously(Boolean purgeSynchronously);

   Integer getPurgerThreads();

   CacheStoreConfig purgerThreads(Integer purgerThreads);

   AsyncStoreConfig asyncStore();

   SingletonStoreConfig singletonStore();

}
