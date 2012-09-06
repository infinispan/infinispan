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
package org.infinispan.loaders.jdbm.configuration;

import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.LoadersConfigurationBuilder;
import org.infinispan.loaders.jdbm.JdbmCacheStore;
import org.infinispan.loaders.jdbm.NaturalComparator;
import org.infinispan.util.TypedProperties;

/**
 * JdbmCacheStoreConfigurationBuilder. Configures a {@link JdbmCacheStore}
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class JdbmCacheStoreConfigurationBuilder extends
      AbstractStoreConfigurationBuilder<JdbmCacheStoreConfiguration, JdbmCacheStoreConfigurationBuilder> {

   private String comparatorClassName = NaturalComparator.class.getName();
   private int expiryQueueSize = 10000;
   private String location = "jdbm";

   public JdbmCacheStoreConfigurationBuilder(LoadersConfigurationBuilder builder) {
      super(builder);
   }

   @Override
   public JdbmCacheStoreConfigurationBuilder self() {
      return this;
   }

   /**
    * Comparator class used to sort the keys by the cache loader. This should only need to be set
    * when using keys that do not have a natural ordering. Defaults to {@link NaturalComparator}
    */
   public JdbmCacheStoreConfigurationBuilder comparatorClassName(String comparatorClassName) {
      this.comparatorClassName = comparatorClassName;
      return this;
   }

   /**
    * Whenever a new entry is stored, an expiry entry is created and added to the a queue that is
    * later consumed by the eviction thread. This parameter sets the size of this queue. Defaults to
    * 10000
    */
   public JdbmCacheStoreConfigurationBuilder expiryQueueSize(int expiryQueueSize) {
      this.expiryQueueSize = expiryQueueSize;
      return this;
   }

   /**
    * A location on disk where the store can write internal files. Defaults to "jdbm"
    */
   public JdbmCacheStoreConfigurationBuilder location(String location) {
      this.location = location;
      return this;
   }

   @Override
   public JdbmCacheStoreConfiguration create() {
      return new JdbmCacheStoreConfiguration(comparatorClassName, expiryQueueSize, location, purgeOnStartup,
            purgeSynchronously, purgerThreads, fetchPersistentState, ignoreModifications,
            TypedProperties.toTypedProperties(properties), async.create(), singletonStore.create());
   }

   @Override
   public JdbmCacheStoreConfigurationBuilder read(JdbmCacheStoreConfiguration template) {
      this.comparatorClassName = template.comparatorClassName();
      this.expiryQueueSize = template.expiryQueueSize();
      this.location = template.location();

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
