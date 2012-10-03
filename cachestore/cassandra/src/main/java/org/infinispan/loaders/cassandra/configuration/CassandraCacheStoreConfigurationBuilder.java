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

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.infinispan.config.ConfigurationException;
import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.LoadersConfigurationBuilder;
import org.infinispan.loaders.keymappers.DefaultTwoWayKey2StringMapper;
import org.infinispan.loaders.keymappers.Key2StringMapper;
import org.infinispan.util.TypedProperties;

/**
 * CassandraCacheStoreConfigurationBuilder. Configures a {@link CassandraCacheStore}
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class CassandraCacheStoreConfigurationBuilder extends
      AbstractStoreConfigurationBuilder<CassandraCacheStoreConfiguration, CassandraCacheStoreConfigurationBuilder>
      implements CassandraCacheStoreConfigurationChildBuilder<CassandraCacheStoreConfigurationBuilder> {

   private boolean autoCreateKeyspace = true;
   private String configurationPropertiesFile;
   private String entryColumnFamily = "InfinispanEntries";
   private String expirationColumnFamily = "InfinispanExpiration";
   private boolean framed = true;
   private List<CassandraServerConfigurationBuilder> servers = new ArrayList<CassandraServerConfigurationBuilder>();
   private String keyMapper = DefaultTwoWayKey2StringMapper.class.getName();
   private String keySpace = "Infinispan";
   private String password;
   private ConsistencyLevel readConsistencyLevel = ConsistencyLevel.ONE;
   private boolean sharedKeyspace = false;
   private String username;
   private ConsistencyLevel writeConsistencyLevel = ConsistencyLevel.ONE;

   public CassandraCacheStoreConfigurationBuilder(LoadersConfigurationBuilder builder) {
      super(builder);
   }

   @Override
   public CassandraCacheStoreConfigurationBuilder self() {
      return this;
   }

   @Override
   public CassandraServerConfigurationBuilder addServer() {
      CassandraServerConfigurationBuilder server = new CassandraServerConfigurationBuilder(this);
      servers.add(server);
      return server;
   }

   @Override
   public CassandraCacheStoreConfigurationBuilder autoCreateKeyspace(boolean autoCreateKeyspace) {
      this.autoCreateKeyspace = autoCreateKeyspace;
      return this;
   }

   @Override
   public CassandraCacheStoreConfigurationBuilder configurationPropertiesFile(String configurationPropertiesFile) {
      this.configurationPropertiesFile = configurationPropertiesFile;
      return this;
   }

   @Override
   public CassandraCacheStoreConfigurationBuilder entryColumnFamily(String entryColumnFamily) {
      this.entryColumnFamily = entryColumnFamily;
      return this;
   }

   @Override
   public CassandraCacheStoreConfigurationBuilder expirationColumnFamily(String expirationColumnFamily) {
      this.expirationColumnFamily = expirationColumnFamily;
      return this;
   }

   @Override
   public CassandraCacheStoreConfigurationBuilder framed(boolean framed) {
      this.framed = framed;
      return this;
   }

   @Override
   public CassandraCacheStoreConfigurationBuilder keyMapper(String keyMapper) {
      this.keyMapper = keyMapper;
      return this;
   }

   @Override
   public CassandraCacheStoreConfigurationBuilder keyMapper(Class<? extends Key2StringMapper> keyMapper) {
      this.keyMapper = keyMapper.getName();
      return this;
   }

   @Override
   public CassandraCacheStoreConfigurationBuilder keySpace(String keySpace) {
      this.keySpace = keySpace;
      return this;
   }

   @Override
   public CassandraCacheStoreConfigurationBuilder password(String password) {
      this.password = password;
      return this;
   }

   @Override
   public CassandraCacheStoreConfigurationBuilder readConsistencyLevel(ConsistencyLevel readConsistencyLevel) {
      this.readConsistencyLevel = readConsistencyLevel;
      return this;
   }

   @Override
   public CassandraCacheStoreConfigurationBuilder username(String username) {
      this.username = username;
      return this;
   }

   @Override
   public CassandraCacheStoreConfigurationBuilder writeConsistencyLevel(ConsistencyLevel writeConsistencyLevel) {
      this.writeConsistencyLevel = writeConsistencyLevel;
      return this;
   }



   @Override
   public void validate() {
      super.validate();
      if (servers.isEmpty()) {
         throw new ConfigurationException("No servers specified");
      }
   }

   @Override
   public CassandraCacheStoreConfiguration create() {
      List<CassandraServerConfiguration> remoteServers = new ArrayList<CassandraServerConfiguration>();
      for (CassandraServerConfigurationBuilder server : servers) {
         remoteServers.add(server.create());
      }
      return new CassandraCacheStoreConfiguration(autoCreateKeyspace, configurationPropertiesFile, entryColumnFamily,
            expirationColumnFamily, framed, remoteServers, keyMapper, keySpace, password, sharedKeyspace, username,
            readConsistencyLevel, writeConsistencyLevel, autoCreateKeyspace, sharedKeyspace, purgerThreads, framed,
            autoCreateKeyspace, TypedProperties.toTypedProperties(properties), async.create(), singletonStore.create());
   }

   @Override
   public CassandraCacheStoreConfigurationBuilder read(CassandraCacheStoreConfiguration template) {
      autoCreateKeyspace = template.autoCreateKeyspace();
      configurationPropertiesFile = template.configurationPropertiesFile();
      entryColumnFamily = template.entryColumnFamily();
      expirationColumnFamily = template.expirationColumnFamily();
      framed = template.framed();
      for(CassandraServerConfiguration server : template.servers()) {
         this.addServer().host(server.host()).port(server.port());
      }
      keyMapper = template.keyMapper();
      keySpace = template.keySpace();
      password = template.password();
      readConsistencyLevel = template.readConsistencyLevel();
      sharedKeyspace = template.sharedKeyspace();
      username = template.username();
      writeConsistencyLevel = template.writeConsistencyLevel();

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
