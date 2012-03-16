/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.loaders.jdbc.stringbased;

import org.infinispan.loaders.jdbc.AbstractNonDelegatingJdbcCacheStoreConfig;
import org.infinispan.loaders.jdbc.TableManipulation;
import org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactoryConfig;
import org.infinispan.loaders.keymappers.DefaultTwoWayKey2StringMapper;
import org.infinispan.loaders.keymappers.Key2StringMapper;
import org.infinispan.util.Util;

/**
 * Configuration for {@link org.infinispan.loaders.jdbc.stringbased.JdbcStringBasedCacheStore} cache store.
 *
 * @author Mircea.Markus@jboss.com
 * @see org.infinispan.loaders.keymappers.Key2StringMapper
 */
public class JdbcStringBasedCacheStoreConfig extends AbstractNonDelegatingJdbcCacheStoreConfig {

   /**
    * The serialVersionUID
    */
   private static final long serialVersionUID = 8835350707132331983L;

   private Key2StringMapper key2StringMapper;

   public JdbcStringBasedCacheStoreConfig(ConnectionFactoryConfig connectionFactoryConfig, TableManipulation tableManipulation) {
      this();
      this.connectionFactoryConfig = connectionFactoryConfig;
      this.tableManipulation = tableManipulation;
   }

   public JdbcStringBasedCacheStoreConfig() {
      cacheLoaderClassName = JdbcStringBasedCacheStore.class.getName();
   }

   public JdbcStringBasedCacheStoreConfig(boolean manageConnectionFactory) {
      this();
      this.manageConnectionFactory = manageConnectionFactory;
   }

   public Key2StringMapper getKey2StringMapper() {
      if (key2StringMapper == null) {
         try {
            key2StringMapper = DefaultTwoWayKey2StringMapper.class.newInstance();
         } catch (Exception e) {
            throw new IllegalStateException("This should never happen", e);
         }
      }
      return key2StringMapper;
   }

   /**
    * Name of the class implementing Key2StringMapper. The default value is {@link org.infinispan.loaders.keymappers.DefaultTwoWayKey2StringMapper}
    *
    * @see org.infinispan.loaders.keymappers.Key2StringMapper
    */
   public void setKey2StringMapperClass(String className) {
      testImmutability("key2StringMapper");
      key2StringMapper = (Key2StringMapper) Util.getInstance(className, getClassLoader());
   }

   /**
    * Sets the prefix for the name of the table where the data will be stored. "_<cache name>" will be appended
    * to this prefix in order to enforce unique table names for each cache.
    */
   public void setStringsTableNamePrefix(String stringsTableNamePrefix) {
      testImmutability("tableManipulation");
      this.tableManipulation.setTableNamePrefix(stringsTableNamePrefix);
   }


   @Override
   public JdbcStringBasedCacheStoreConfig clone() {
      JdbcStringBasedCacheStoreConfig result = (JdbcStringBasedCacheStoreConfig) super.clone();
      result.key2StringMapper = key2StringMapper;
      return result;
   }

   @Override
   public String toString() {
      return "JdbcStringBasedCacheStoreConfig{" +
            "key2StringMapper=" + key2StringMapper +
            "} " + super.toString();
   }
}
