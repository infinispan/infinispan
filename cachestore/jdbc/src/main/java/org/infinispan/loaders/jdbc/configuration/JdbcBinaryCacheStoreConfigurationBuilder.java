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
import org.infinispan.util.TypedProperties;

public class JdbcBinaryCacheStoreConfigurationBuilder extends
      AbstractJdbcCacheStoreConfigurationBuilder<JdbcBinaryCacheStoreConfiguration, JdbcBinaryCacheStoreConfigurationBuilder> {
   protected final TableManipulationConfigurationBuilder table;

   public JdbcBinaryCacheStoreConfigurationBuilder(LoadersConfigurationBuilder builder) {
      super(builder);
      this.table = new TableManipulationConfigurationBuilder(this);
   }

   @Override
   public JdbcBinaryCacheStoreConfigurationBuilder self() {
      return this;
   }

   public TableManipulationConfigurationBuilder table() {
      return table;
   }

   @Override
   public void validate() {
      super.validate();
   }

   @Override
   public JdbcBinaryCacheStoreConfiguration create() {
      return new JdbcBinaryCacheStoreConfiguration(table.create(), driverClass, connectionUrl, username, password,
            connectionFactoryClass, datasource, lockAcquistionTimeout, lockConcurrencyLevel,
            purgeOnStartup, purgeSynchronously, purgerThreads, fetchPersistentState, ignoreModifications,
            TypedProperties.toTypedProperties(properties), async.create(), singletonStore.create());
   }

   @Override
   public JdbcBinaryCacheStoreConfigurationBuilder read(JdbcBinaryCacheStoreConfiguration template) {
      super.readInternal(template);
      this.table.read(template.table());

      return this;
   }
}
