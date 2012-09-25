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
package org.infinispan.loaders.hbase.configuration;

import org.infinispan.configuration.BuiltBy;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.LegacyConfigurationAdaptor;
import org.infinispan.configuration.cache.LegacyLoaderAdapter;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;
import org.infinispan.loaders.hbase.HBaseCacheStoreConfig;
import org.infinispan.util.TypedProperties;

@BuiltBy(HBaseCacheStoreConfigurationBuilder.class)
public class HBaseCacheStoreConfiguration extends AbstractStoreConfiguration implements LegacyLoaderAdapter<HBaseCacheStoreConfig> {

   private final boolean autoCreateTable;
   private final String entryColumnFamily;
   private final String entryTable;
   private final String entryValueField;
   private final String expirationColumnFamily;
   private final String expirationTable;
   private final String expirationValueField;
   private final String hbaseZookeeperQuorumHost;
   private final int hbaseZookeeperClientPort;
   private final String keyMapper;
   private final boolean sharedTable;

   public HBaseCacheStoreConfiguration(boolean autoCreateTable, String entryColumnFamily, String entryTable, String entryValueField, String expirationColumnFamily,
         String expirationTable, String expirationValueField, String hbaseZookeeperQuorumHost, int hbaseZookeeperClientPort, String keyMapper, boolean sharedTable,
         boolean purgeOnStartup, boolean purgeSynchronously, int purgerThreads, boolean fetchPersistentState, boolean ignoreModifications, TypedProperties properties,
         AsyncStoreConfiguration asyncStoreConfiguration, SingletonStoreConfiguration singletonStoreConfiguration) {
      super(purgeOnStartup, purgeSynchronously, purgerThreads, fetchPersistentState, ignoreModifications, properties, asyncStoreConfiguration, singletonStoreConfiguration);
      this.autoCreateTable = autoCreateTable;
      this.entryColumnFamily = entryColumnFamily;
      this.entryTable = entryTable;
      this.entryValueField = entryValueField;
      this.expirationColumnFamily = expirationColumnFamily;
      this.expirationTable = expirationTable;
      this.expirationValueField = expirationValueField;
      this.hbaseZookeeperQuorumHost = hbaseZookeeperQuorumHost;
      this.hbaseZookeeperClientPort = hbaseZookeeperClientPort;
      this.keyMapper = keyMapper;
      this.sharedTable = sharedTable;
   }

   public boolean autoCreateTable() {
      return autoCreateTable;
   }

   public String entryColumnFamily() {
      return entryColumnFamily;
   }

   public String entryTable() {
      return entryTable;
   }

   public String entryValueField() {
      return entryValueField;
   }

   public String expirationColumnFamily() {
      return expirationColumnFamily;
   }

   public String expirationTable() {
      return expirationTable;
   }

   public String expirationValueField() {
      return expirationValueField;
   }

   public String hbaseZookeeperQuorumHost() {
      return hbaseZookeeperQuorumHost;
   }

   public int hbaseZookeeperClientPort() {
      return hbaseZookeeperClientPort;
   }

   public String keyMapper() {
      return keyMapper;
   }

   public boolean sharedTable() {
      return sharedTable;
   }

   @Override
   public HBaseCacheStoreConfig adapt() {
      HBaseCacheStoreConfig config = new HBaseCacheStoreConfig();

      LegacyConfigurationAdaptor.adapt(this, config);

      config.setAutoCreateTable(autoCreateTable);
      config.setEntryColumnFamily(entryColumnFamily);
      config.setEntryTable(entryTable);
      config.setEntryValueField(entryValueField);
      config.setExpirationColumnFamily(expirationColumnFamily);
      config.setExpirationTable(expirationTable);
      config.setExpirationValueField(expirationValueField);
      config.setHbaseZookeeperPropertyClientPort(hbaseZookeeperClientPort);
      config.setHbaseZookeeperQuorum(hbaseZookeeperQuorumHost);
      config.setKeyMapper(keyMapper);
      config.setSharedTable(sharedTable);

      return config;
   }

}
