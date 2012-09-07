/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.loaders.dummy;

import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.LoadersConfigurationBuilder;
import org.infinispan.util.TypedProperties;

public class DummyInMemoryCacheStoreConfigurationBuilder
      extends
      AbstractStoreConfigurationBuilder<DummyInMemoryCacheStoreConfiguration, DummyInMemoryCacheStoreConfigurationBuilder> {

   private boolean debug;
   private String storeName;
   private Object failKey;

   public DummyInMemoryCacheStoreConfigurationBuilder(LoadersConfigurationBuilder builder) {
      super(builder);
   }

   @Override
   public DummyInMemoryCacheStoreConfigurationBuilder self() {
      return this;
   }

   public DummyInMemoryCacheStoreConfigurationBuilder debug(boolean debug) {
      this.debug = debug;
      return this;
   }

   public DummyInMemoryCacheStoreConfigurationBuilder storeName(String storeName) {
      this.storeName = storeName;
      return this;
   }

   public DummyInMemoryCacheStoreConfigurationBuilder failKey(Object failKey) {
      this.failKey = failKey;
      return this;
   }

   @Override
   public DummyInMemoryCacheStoreConfiguration create() {
      return new DummyInMemoryCacheStoreConfiguration(debug, storeName, failKey, purgeOnStartup, purgeSynchronously, purgerThreads,
            fetchPersistentState, ignoreModifications, TypedProperties.toTypedProperties(properties), async.create(),
            singletonStore.create());
   }

   @Override
   public DummyInMemoryCacheStoreConfigurationBuilder read(DummyInMemoryCacheStoreConfiguration template) {
      // AbstractStore-specific configuration
      fetchPersistentState = template.fetchPersistentState();
      ignoreModifications = template.ignoreModifications();
      properties = template.properties();
      purgeOnStartup = template.purgeOnStartup();
      purgeSynchronously = template.purgeSynchronously();
      async.read(template.async());
      singletonStore.read(template.singletonStore());

      return this;
   }

}
