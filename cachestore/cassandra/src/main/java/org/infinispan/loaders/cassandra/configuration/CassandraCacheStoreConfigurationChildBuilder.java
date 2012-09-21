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
package org.infinispan.loaders.cassandra.configuration;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.infinispan.configuration.cache.StoreConfigurationChildBuilder;
import org.infinispan.loaders.keymappers.Key2StringMapper;

/**
 * CassandraCacheStoreConfigurationChildBuilder.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public interface CassandraCacheStoreConfigurationChildBuilder extends StoreConfigurationChildBuilder {
   CassandraCacheStoreConfigurationBuilder autoCreateKeyspace(boolean autoCreateKeyspace);

   CassandraCacheStoreConfigurationBuilder configurationPropertiesFile(String configurationPropertiesFile);

   CassandraCacheStoreConfigurationBuilder entryColumnFamily(String entryColumnFamily);

   CassandraCacheStoreConfigurationBuilder expirationColumnFamily(String expirationColumnFamily);

   CassandraCacheStoreConfigurationBuilder framed(boolean framed);

   CassandraCacheStoreConfigurationBuilder keyMapper(String keyMapper);

   CassandraCacheStoreConfigurationBuilder keyMapper(Class<? extends Key2StringMapper> keyMapper);

   CassandraCacheStoreConfigurationBuilder keySpace(String keySpace);

   CassandraCacheStoreConfigurationBuilder password(String password);

   CassandraCacheStoreConfigurationBuilder readConsistencyLevel(ConsistencyLevel readConsistencyLevel);

   CassandraCacheStoreConfigurationBuilder username(String username);

   CassandraCacheStoreConfigurationBuilder writeConsistencyLevel(ConsistencyLevel writeConsistencyLevel);

   CassandraServerConfigurationBuilder addServer();
}
