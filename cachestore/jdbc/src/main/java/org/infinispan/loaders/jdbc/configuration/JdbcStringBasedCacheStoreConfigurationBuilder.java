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
package org.infinispan.loaders.jdbc.configuration;

import org.infinispan.configuration.Builder;
import org.infinispan.configuration.cache.LoadersConfigurationBuilder;
import org.infinispan.loaders.keymappers.DefaultTwoWayKey2StringMapper;
import org.infinispan.loaders.keymappers.Key2StringMapper;
import org.infinispan.util.TypedProperties;

/**
 *
 * JdbcStringBasedCacheStoreConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class JdbcStringBasedCacheStoreConfigurationBuilder
      extends
      AbstractJdbcCacheStoreConfigurationBuilder<JdbcStringBasedCacheStoreConfiguration, JdbcStringBasedCacheStoreConfigurationBuilder> {
   private String key2StringMapper = DefaultTwoWayKey2StringMapper.class.getName();
   private TableManipulationConfigurationBuilder table;

   public JdbcStringBasedCacheStoreConfigurationBuilder(LoadersConfigurationBuilder builder) {
      super(builder);
      table = new TableManipulationConfigurationBuilder(this);
   }

   @Override
   public JdbcStringBasedCacheStoreConfigurationBuilder self() {
      return this;
   }

   public JdbcStringBasedCacheStoreConfigurationBuilder key2StringMapper(String key2StringMapper) {
      this.key2StringMapper = key2StringMapper;
      return this;
   }

   public JdbcStringBasedCacheStoreConfigurationBuilder key2StringMapper(Class<? extends Key2StringMapper> klass) {
      this.key2StringMapper = klass.getName();
      return this;
   }

   public TableManipulationConfigurationBuilder table() {
      return table;
   }

   @Override
   public void validate() {
   }

   @Override
   public JdbcStringBasedCacheStoreConfiguration create() {
      return new JdbcStringBasedCacheStoreConfiguration(key2StringMapper, table.create(), driverClass, connectionUrl,
            username, password, connectionFactoryClass, datasource, lockAcquistionTimeout, lockConcurrencyLevel,
            purgeOnStartup, purgeSynchronously, purgerThreads, fetchPersistentState, ignoreModifications,
            TypedProperties.toTypedProperties(properties), async.create(), singletonStore.create());
   }

   @Override
   public Builder<?> read(JdbcStringBasedCacheStoreConfiguration template) {
      super.readInternal(template);
      this.key2StringMapper = template.key2StringMapper();
      this.table.read(template.table());
      return this;
   }

}
