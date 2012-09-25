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

import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.LoadersConfigurationBuilder;
import org.infinispan.loaders.hbase.HBaseCacheStore;
import org.infinispan.loaders.keymappers.MarshalledValueOrPrimitiveMapper;
import org.infinispan.loaders.keymappers.TwoWayKey2StringMapper;
import org.infinispan.util.TypedProperties;

/**
 * HBaseCacheStoreConfigurationBuilder. Configures a {@link HBaseCacheStore}
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class HBaseCacheStoreConfigurationBuilder extends AbstractStoreConfigurationBuilder<HBaseCacheStoreConfiguration, HBaseCacheStoreConfigurationBuilder> {

   private boolean autoCreateTable = true;
   private String entryColumnFamily = "E";
   private String entryTable = "ISPNCacheStore";
   private String entryValueField = "EV";
   private String expirationColumnFamily = "X";
   private String expirationTable = "ISPNCacheStoreExpiration";
   private String expirationValueField = "XV";
   private String hbaseZookeeperQuorumHost = "localhost";
   private int hbaseZookeeperClientPort = 2181;
   private String keyMapper = MarshalledValueOrPrimitiveMapper.class.getName();
   private boolean sharedTable = false;

   public HBaseCacheStoreConfigurationBuilder(LoadersConfigurationBuilder builder) {
      super(builder);
   }

   @Override
   public HBaseCacheStoreConfigurationBuilder self() {
      return this;
   }

   /**
    * Whether to automatically create the HBase table with the appropriate column families (true by
    * default).
    */
   public HBaseCacheStoreConfigurationBuilder autoCreateTable(boolean autoCreateTable) {
      this.autoCreateTable = autoCreateTable;
      return this;
   }

   /**
    * The column family for entries. Defaults to 'E'
    */
   public HBaseCacheStoreConfigurationBuilder entryColumnFamily(String entryColumnFamily) {
      this.entryColumnFamily = entryColumnFamily;
      return this;
   }

   /**
    * The HBase table for storing the cache entries. Defaults to 'ISPNCacheStore'
    */
   public HBaseCacheStoreConfigurationBuilder entryTable(String entryTable) {
      this.entryTable = entryTable;
      return this;
   }

   /**
    * The field name containing the entries. Defaults to 'EV'
    */
   public HBaseCacheStoreConfigurationBuilder entryValueField(String entryValueField) {
      this.entryValueField = entryValueField;
      return this;
   }

   /**
    * The column family for expiration metadata. Defaults to 'X'
    */
   public HBaseCacheStoreConfigurationBuilder expirationColumnFamily(String expirationColumnFamily) {
      this.expirationColumnFamily = expirationColumnFamily;
      return this;
   }

   /**
    * The HBase table for storing the cache expiration metadata. Defaults to
    * 'ISPNCacheStoreExpiration'
    */
   public HBaseCacheStoreConfigurationBuilder expirationTable(String expirationTable) {
      this.expirationTable = expirationTable;
      return this;
   }

   /**
    * The field name containing the expiration metadata. Defaults to 'XV'
    */
   public HBaseCacheStoreConfigurationBuilder expirationValueField(String expirationValueField) {
      this.expirationValueField = expirationValueField;
      return this;
   }

   /**
    * The HBase zookeeper client port. Defaults to 'localhost'
    */
   public HBaseCacheStoreConfigurationBuilder hbaseZookeeperQuorumHost(String hbaseZookeeperQuorumHost) {
      this.hbaseZookeeperQuorumHost = hbaseZookeeperQuorumHost;
      return this;
   }

   /**
    * The HBase zookeeper client port. Defaults to '2181'
    */
   public HBaseCacheStoreConfigurationBuilder hbaseZookeeperClientPort(int hbaseZookeeperClientPort) {
      this.hbaseZookeeperClientPort = hbaseZookeeperClientPort;
      return this;
   }

   /**
    * The keymapper for converting keys to strings (uses the
    * {@link MarshalledValueOrPrimitiveMapper} by default)
    */
   public HBaseCacheStoreConfigurationBuilder keyMapper(String keyMapper) {
      this.keyMapper = keyMapper;
      return this;
   }

   /**
    * The keymapper for converting keys to strings (uses the
    * {@link MarshalledValueOrPrimitiveMapper} by default)
    */
   public HBaseCacheStoreConfigurationBuilder keyMapper(Class<? extends TwoWayKey2StringMapper> keyMapper) {
      this.keyMapper = keyMapper.getName();
      return this;
   }

   /**
    * Whether the table is shared between multiple caches. Defaults to 'false'
    */
   public HBaseCacheStoreConfigurationBuilder sharedTable(boolean sharedTable) {
      this.sharedTable = sharedTable;
      return this;
   }

   @Override
   public HBaseCacheStoreConfiguration create() {
      return new HBaseCacheStoreConfiguration(autoCreateTable, entryColumnFamily, entryTable, entryValueField, expirationColumnFamily, expirationTable, expirationValueField,
            hbaseZookeeperQuorumHost, hbaseZookeeperClientPort, keyMapper, sharedTable, purgeOnStartup, purgeSynchronously, purgerThreads, fetchPersistentState,
            ignoreModifications, TypedProperties.toTypedProperties(properties), async.create(), singletonStore.create());
   }

   @Override
   public HBaseCacheStoreConfigurationBuilder read(HBaseCacheStoreConfiguration template) {
      autoCreateTable = template.autoCreateTable();
      entryColumnFamily = template.entryColumnFamily();
      entryTable = template.entryTable();
      entryValueField = template.entryValueField();
      expirationColumnFamily = template.expirationColumnFamily();
      expirationTable = template.expirationTable();
      expirationValueField = template.expirationValueField();
      hbaseZookeeperQuorumHost = template.hbaseZookeeperQuorumHost();
      hbaseZookeeperClientPort = template.hbaseZookeeperClientPort();
      keyMapper = template.keyMapper();
      sharedTable = template.sharedTable();

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
