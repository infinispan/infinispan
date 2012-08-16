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
package org.infinispan.configuration.cache;

import org.infinispan.loaders.CacheLoader;
import org.infinispan.loaders.CacheStore;
import org.infinispan.util.TypedProperties;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Configuration of a legacy cache store, i.e. a store which is still configured via properties and
 * does not yet provide a builder interface
 *
 * @author Pete Muir
 * @author Tristan Tarrant
 *
 * @since 5.2
 */
public class LegacyStoreConfigurationBuilder extends AbstractStoreConfigurationBuilder<LegacyStoreConfiguration, LegacyStoreConfigurationBuilder> implements
      StoreConfigurationChildBuilder {

   private static final Log log = LogFactory.getLog(LegacyStoreConfigurationBuilder.class);

   private CacheLoader cacheStore; // TODO: in 6.0, as we deprecate the cacheLoader() method, narrow this type to CacheStore

   LegacyStoreConfigurationBuilder(LoadersConfigurationBuilder builder) {
      super(builder);
   }

   @Override
   public LegacyStoreConfigurationBuilder self() {
      return this;
   }



   @Deprecated
   public LegacyStoreConfigurationBuilder cacheLoader(CacheLoader cacheLoader) {
      this.cacheStore = cacheLoader;
      return this;
   }

   /**
    * NOTE: Currently Infinispan will not use the object instance, but instead instantiate a new
    * instance of the class. Therefore, do not expect any state to survive, and provide a no-args
    * constructor to any instance. This will be resolved in Infinispan 5.2.0
    *
    * @param cacheLoader
    * @return
    */
   public LegacyStoreConfigurationBuilder cacheStore(CacheStore cacheStore) {
      this.cacheStore = cacheStore;
      return this;
   }

   @Override
   public void validate() {
      async.validate();
      singletonStore.validate();
      if (!loaders().shared() && !fetchPersistentState && !purgeOnStartup
            && getBuilder().clustering().cacheMode().isClustered())
         log.staleEntriesWithoutFetchPersistentStateOrPurgeOnStartup();
   }

   @Override
   public LegacyStoreConfiguration create() {
      return new LegacyStoreConfiguration(TypedProperties.toTypedProperties(properties), cacheStore, fetchPersistentState,
            ignoreModifications, purgeOnStartup, purgerThreads, purgeSynchronously, async.create(), singletonStore.create());
   }

   @Override
   public LegacyStoreConfigurationBuilder read(LegacyStoreConfiguration template) {
      this.cacheStore = template.cacheStore();
      this.fetchPersistentState = template.fetchPersistentState();
      this.ignoreModifications = template.ignoreModifications();
      this.properties = template.properties();
      this.purgeOnStartup = template.purgeOnStartup();
      this.purgerThreads = template.purgerThreads();
      this.purgeSynchronously = template.purgeSynchronously();

      this.async.read(template.async());
      this.singletonStore.read(template.singletonStore());

      return this;
   }

   @Override
   public String toString() {
      return "StoreConfigurationBuilder{" +
            "cacheStore=" + cacheStore +
            ", fetchPersistentState=" + fetchPersistentState +
            ", ignoreModifications=" + ignoreModifications +
            ", purgeOnStartup=" + purgeOnStartup +
            ", purgerThreads=" + purgerThreads +
            ", purgeSynchronously=" + purgeSynchronously +
            ", properties=" + properties +
            ", async=" + async +
            ", singletonStore=" + singletonStore +
            '}';
   }

}
