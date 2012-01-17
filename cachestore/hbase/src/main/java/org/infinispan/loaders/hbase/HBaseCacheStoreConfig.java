/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.loaders.hbase;

import org.infinispan.loaders.LockSupportCacheStoreConfig;
import org.infinispan.loaders.keymappers.MarshalledValueOrPrimitiveMapper;

/**
 * Configures {@link HBaseCacheStore}.
 */
public class HBaseCacheStoreConfig extends LockSupportCacheStoreConfig {

   /** The serialVersionUID */
   private static final long serialVersionUID = -7845734960711045535L;

   /**
    * @configRef desc="The server name of the HBase zookeeper quorum."
    */
   String hbaseZookeeperQuorum = "localhost";

   /**
    * @configRef desc="The HBase zookeeper client port."
    */
   String hbaseZookeeperPropertyClientPort = "2181";

   /**
    * @configRef desc="The HBase table for storing the cache entries"
    */
   String entryTable = "ISPNCacheStore";

   /**
    * @configRef desc="The column family for entries"
    */
   String entryColumnFamily = "E";

   /**
    * @configRef desc="The field name containing the entries"
    */
   String entryValueField = "EV";

   /**
    * @configRef desc="The HBase table for storing the cache expiration metadata"
    */
   String expirationTable = "ISPNCacheStoreExpiration";

   /**
    * @configRef desc="The column family for expirations"
    */
   String expirationColumnFamily = "X";

   /**
    * @configRef desc="The field name containing the entries"
    */
   String expirationValueField = "XV";

   /**
    * @configRef desc="Whether the table is shared between multiple caches"
    */
   boolean sharedTable = false;

   /**
    * @configRef desc=
    *            "Whether to automatically create the HBase table with the appropriate column families (true by default)"
    */
   boolean autoCreateTable = true;

   /**
    * @configRef desc=
    *            "The keymapper for converting keys to strings (uses the MarshalledValueOrPrimitiveMapper by default)"
    */
   String keyMapper = MarshalledValueOrPrimitiveMapper.class.getName();

   public HBaseCacheStoreConfig() {
      setCacheLoaderClassName(HBaseCacheStore.class.getName());
   }

   public String getHbaseRootDir() {
      return hbaseZookeeperQuorum;
   }

   public void setHbaseZookeeperQuorum(String hbaseZookeeperQuorum) {
      this.hbaseZookeeperQuorum = hbaseZookeeperQuorum;
   }

   public String getHbaseZookeeperPropertyClientPort() {
      return hbaseZookeeperPropertyClientPort;
   }

   public void setHbaseZookeeperPropertyClientPort(String hbaseZookeeperPropertyClientPort) {
      this.hbaseZookeeperPropertyClientPort = hbaseZookeeperPropertyClientPort;
   }

   public String getEntryTable() {
      return entryTable;
   }

   public void setEntryTable(String entryTable) {
      this.entryTable = entryTable;
   }

   public String getEntryColumnFamily() {
      return entryColumnFamily;
   }

   public void setEntryColumnFamily(String entryColumnFamily) {
      this.entryColumnFamily = entryColumnFamily;
   }

   public String getEntryValueField() {
      return entryValueField;
   }

   public void setEntryValueField(String entryValueField) {
      this.entryValueField = entryValueField;
   }

   public String getExpirationTable() {
      return expirationTable;
   }

   public void setExpirationTable(String expirationTable) {
      this.expirationTable = expirationTable;
   }

   public String getExpirationColumnFamily() {
      return expirationColumnFamily;
   }

   public void setExpirationColumnFamily(String expirationColumnFamily) {
      this.expirationColumnFamily = expirationColumnFamily;
   }

   public String getExpirationValueField() {
      return expirationValueField;
   }

   public void setExpirationValueField(String expirationValueField) {
      this.expirationValueField = expirationValueField;
   }

   public boolean isSharedTable() {
      return sharedTable;
   }

   public void setSharedTable(boolean sharedTable) {
      this.sharedTable = sharedTable;
   }

   public String getKeyMapper() {
      return keyMapper;
   }

   public void setKeyMapper(String keyMapper) {
      this.keyMapper = keyMapper;
   }

   public boolean isAutoCreateTable() {
      return autoCreateTable;
   }

   public void setAutoCreateTable(boolean autoCreateTable) {
      this.autoCreateTable = autoCreateTable;
   }

}
