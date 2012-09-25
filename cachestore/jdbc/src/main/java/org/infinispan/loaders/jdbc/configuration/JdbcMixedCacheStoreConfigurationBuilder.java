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

import org.infinispan.configuration.cache.LoadersConfigurationBuilder;
import org.infinispan.loaders.keymappers.DefaultTwoWayKey2StringMapper;
import org.infinispan.loaders.keymappers.Key2StringMapper;
import org.infinispan.util.TypedProperties;

/**
 *
 * JdbcMixedCacheStoreConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class JdbcMixedCacheStoreConfigurationBuilder extends AbstractJdbcCacheStoreConfigurationBuilder<JdbcMixedCacheStoreConfiguration, JdbcMixedCacheStoreConfigurationBuilder>
      implements JdbcMixedCacheStoreConfigurationChildBuilder<JdbcMixedCacheStoreConfigurationBuilder> {
   private final MixedTableManipulationConfigurationBuilder binaryTable;
   private final MixedTableManipulationConfigurationBuilder stringTable;
   private String key2StringMapper = DefaultTwoWayKey2StringMapper.class.getName();

   public JdbcMixedCacheStoreConfigurationBuilder(LoadersConfigurationBuilder builder) {
      super(builder);
      this.binaryTable = new MixedTableManipulationConfigurationBuilder(this);
      this.stringTable = new MixedTableManipulationConfigurationBuilder(this);
   }

   @Override
   public JdbcMixedCacheStoreConfigurationBuilder self() {
      return this;
   }

   /**
    * Allows configuration of table-specific parameters such as column names and types for the table
    * used to store entries with binary keys
    */
   @Override
   public MixedTableManipulationConfigurationBuilder binaryTable() {
      return binaryTable;
   }

   /**
    * Allows configuration of table-specific parameters such as column names and types for the table
    * used to store entries with string keys
    */
   @Override
   public MixedTableManipulationConfigurationBuilder stringTable() {
      return stringTable;
   }

   /**
    * The class name of a {@link Key2StringMapper} to use for mapping keys to strings suitable for
    * storage in a database table. Defaults to {@link DefaultTwoWayKey2StringMapper}
    */
   @Override
   public JdbcMixedCacheStoreConfigurationChildBuilder<JdbcMixedCacheStoreConfigurationBuilder> key2StringMapper(String key2StringMapper) {
      this.key2StringMapper = key2StringMapper;
      return this;
   }

   /**
    * The class of a {@link Key2StringMapper} to use for mapping keys to strings suitable for
    * storage in a database table. Defaults to {@link DefaultTwoWayKey2StringMapper}
    */
   @Override
   public JdbcMixedCacheStoreConfigurationChildBuilder<JdbcMixedCacheStoreConfigurationBuilder> key2StringMapper(Class<? extends Key2StringMapper> klass) {
      this.key2StringMapper = klass.getName();
      return this;
   }

   @Override
   public void validate() {
   }

   @Override
   public JdbcMixedCacheStoreConfiguration create() {
      return new JdbcMixedCacheStoreConfiguration(key2StringMapper, binaryTable.create(), stringTable.create(), connectionFactory.create(), lockAcquistionTimeout,
            lockConcurrencyLevel, purgeOnStartup, purgeSynchronously, purgerThreads, fetchPersistentState, ignoreModifications, TypedProperties.toTypedProperties(properties),
            async.create(), singletonStore.create());
   }

   @Override
   public JdbcMixedCacheStoreConfigurationBuilder read(JdbcMixedCacheStoreConfiguration template) {
      super.readInternal(template);
      this.binaryTable.read(template.binaryTable());
      this.stringTable.read(template.stringTable());
      this.key2StringMapper = template.key2StringMapper();
      return this;
   }

   public class MixedTableManipulationConfigurationBuilder extends
         TableManipulationConfigurationBuilder<JdbcMixedCacheStoreConfigurationBuilder, MixedTableManipulationConfigurationBuilder> implements
         JdbcMixedCacheStoreConfigurationChildBuilder<JdbcMixedCacheStoreConfigurationBuilder> {

      MixedTableManipulationConfigurationBuilder(AbstractJdbcCacheStoreConfigurationBuilder<?, JdbcMixedCacheStoreConfigurationBuilder> builder) {
         super(builder);
      }

      @Override
      public MixedTableManipulationConfigurationBuilder self() {
         return this;
      }

      @Override
      public MixedTableManipulationConfigurationBuilder binaryTable() {
         return binaryTable;
      }

      @Override
      public MixedTableManipulationConfigurationBuilder stringTable() {
         return stringTable;
      }

      @Override
      public PooledConnectionFactoryConfigurationBuilder<JdbcMixedCacheStoreConfigurationBuilder> connectionPool() {
         return JdbcMixedCacheStoreConfigurationBuilder.this.connectionPool();
      }

      @Override
      public ManagedConnectionFactoryConfigurationBuilder<JdbcMixedCacheStoreConfigurationBuilder> dataSource() {
         return JdbcMixedCacheStoreConfigurationBuilder.this.dataSource();
      }

      @Override
      public JdbcMixedCacheStoreConfigurationChildBuilder<JdbcMixedCacheStoreConfigurationBuilder> key2StringMapper(String key2StringMapper) {
         return JdbcMixedCacheStoreConfigurationBuilder.this.key2StringMapper(key2StringMapper);
      }

      @Override
      public JdbcMixedCacheStoreConfigurationChildBuilder<JdbcMixedCacheStoreConfigurationBuilder> key2StringMapper(Class<? extends Key2StringMapper> klass) {
         return JdbcMixedCacheStoreConfigurationBuilder.this.key2StringMapper(klass);
      }
   }
}
