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
package org.infinispan.loaders.cassandra;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import net.dataforte.cassandra.pool.PoolProperties;

import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.LockSupportCacheStoreConfig;
import org.infinispan.loaders.keymappers.DefaultTwoWayKey2StringMapper;
import org.infinispan.util.FileLookupFactory;
import org.infinispan.util.Util;

/**
 * Configures {@link CassandraCacheStore}.
 */
public class CassandraCacheStoreConfig extends LockSupportCacheStoreConfig {

   /**
    * @configRef desc="The Cassandra keyspace"
    */
   String keySpace = "Infinispan";

   /**
    * @configRef desc="The Cassandra column family for entries"
    */
   String entryColumnFamily = "InfinispanEntries";

   /**
    * @configRef desc="The Cassandra column family for expirations"
    */
   String expirationColumnFamily = "InfinispanExpiration";

   /**
    * @configRef desc="Whether the keySpace is shared between multiple caches"
    */
   boolean sharedKeyspace = false;

   /**
    * @configRef desc="Which Cassandra consistency level to use when reading"
    */
   String readConsistencyLevel = "ONE";

   /**
    * @configRef desc="Which Cassandra consistency level to use when writing"
    */
   String writeConsistencyLevel = "ONE";

   /**
    * @configRef desc=
    *            "An optional properties file for configuring the underlying cassandra connection pool"
    */
   String configurationPropertiesFile;

   /**
    * @configRef desc=
    *            "The keymapper for converting keys to strings (uses the DefaultTwoWayKey2Stringmapper by default)"
    */
   String keyMapper = DefaultTwoWayKey2StringMapper.class.getName();

   /**
    * @configRef desc=
    *            "Whether to automatically create the keyspace with the appropriate column families (true by default)"
    */
   boolean autoCreateKeyspace = true;

   protected PoolProperties poolProperties;

   public CassandraCacheStoreConfig() {
      setCacheLoaderClassName(CassandraCacheStore.class.getName());
      poolProperties = new PoolProperties();
   }

   public String getKeySpace() {
      return keySpace;
   }

   public void setKeySpace(String keySpace) {
      this.keySpace = keySpace;
   }

   public String getEntryColumnFamily() {
      return entryColumnFamily;
   }

   public void setEntryColumnFamily(String entryColumnFamily) {
      this.entryColumnFamily = entryColumnFamily;
   }

   public String getExpirationColumnFamily() {
      return expirationColumnFamily;
   }

   public void setExpirationColumnFamily(String expirationColumnFamily) {
      this.expirationColumnFamily = expirationColumnFamily;
   }

   public boolean isSharedKeyspace() {
      return sharedKeyspace;
   }

   public void setSharedKeyspace(boolean sharedKeyspace) {
      this.sharedKeyspace = sharedKeyspace;
   }

   public String getReadConsistencyLevel() {
      return readConsistencyLevel;
   }

   public void setReadConsistencyLevel(String readConsistencyLevel) {
      this.readConsistencyLevel = readConsistencyLevel;
   }

   public String getWriteConsistencyLevel() {
      return writeConsistencyLevel;
   }

   public void setWriteConsistencyLevel(String writeConsistencyLevel) {
      this.writeConsistencyLevel = writeConsistencyLevel;
   }

   public PoolProperties getPoolProperties() {
      return poolProperties;
   }

   public void setHost(String host) {
      poolProperties.setHost(host);
   }

   public String getHost() {
      return poolProperties.getHost();
   }

   public void setPort(int port) {
      poolProperties.setPort(port);
   }

   public int getPort() {
      return poolProperties.getPort();
   }

   public boolean isFramed() {
      return poolProperties.isFramed();
   }

   public String getPassword() {
      return poolProperties.getPassword();
   }

   public String getUsername() {
      return poolProperties.getUsername();
   }

   public void setFramed(boolean framed) {
      poolProperties.setFramed(framed);

   }

   public void setPassword(String password) {
      poolProperties.setPassword(password);
   }

   public void setUsername(String username) {
      poolProperties.setUsername(username);
   }

   public void setDatasourceJndiLocation(String location) {
      poolProperties.setDataSourceJNDI(location);
   }

   public String getDatasourceJndiLocation() {
      return poolProperties.getDataSourceJNDI();
   }

   public String getConfigurationPropertiesFile() {
      return configurationPropertiesFile;
   }

   public void setConfigurationPropertiesFile(String configurationPropertiesFile)
            throws CacheLoaderException {
      this.configurationPropertiesFile = configurationPropertiesFile;
      readConfigurationProperties();
   }

   private void readConfigurationProperties() throws CacheLoaderException {
      if (configurationPropertiesFile == null || configurationPropertiesFile.trim().length() == 0)
         return;
      InputStream i = FileLookupFactory.newInstance().lookupFile(configurationPropertiesFile, getClassLoader());
      if (i != null) {
         Properties p = new Properties();
         try {
            p.load(i);
         } catch (IOException ioe) {
            throw new CacheLoaderException("Unable to read environment properties file "
                     + configurationPropertiesFile, ioe);
         } finally {
            Util.close(i);
         }

         // Apply all properties to the PoolProperties object
         for (String propertyName : p.stringPropertyNames()) {
            poolProperties.set(propertyName, p.getProperty(propertyName));
         }
      }
   }

   public String getKeyMapper() {
      return keyMapper;
   }

   public void setKeyMapper(String keyMapper) {
      this.keyMapper = keyMapper;
   }

   public boolean isAutoCreateKeyspace() {
      return autoCreateKeyspace;
   }

   public void setAutoCreateKeyspace(boolean autoCreateKeyspace) {
      this.autoCreateKeyspace = autoCreateKeyspace;
   }
   
   
}
