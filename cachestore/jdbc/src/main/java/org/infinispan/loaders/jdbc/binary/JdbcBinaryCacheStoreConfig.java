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
package org.infinispan.loaders.jdbc.binary;

import org.infinispan.loaders.jdbc.AbstractNonDelegatingJdbcCacheStoreConfig;
import org.infinispan.loaders.jdbc.TableManipulation;
import org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactoryConfig;

/**
 * Defines available configuration elements for {@link org.infinispan.loaders.jdbc.binary.JdbcBinaryCacheStore}.
 *
 * @author Mircea.Markus@jboss.com
 */
public class JdbcBinaryCacheStoreConfig extends AbstractNonDelegatingJdbcCacheStoreConfig {

   /** The serialVersionUID */
   private static final long serialVersionUID = 7659899424935453635L;

   public JdbcBinaryCacheStoreConfig(boolean manageConnectionFactory) {
      this.manageConnectionFactory = manageConnectionFactory;
   }

   public JdbcBinaryCacheStoreConfig(ConnectionFactoryConfig connectionFactoryConfig, TableManipulation tm) {
      super(connectionFactoryConfig, tm);
      this.cacheLoaderClassName = JdbcBinaryCacheStore.class.getName();
      this.connectionFactoryConfig = connectionFactoryConfig;
      this.tableManipulation = tm;
   }

   public JdbcBinaryCacheStoreConfig() {
      cacheLoaderClassName = JdbcBinaryCacheStore.class.getName();
   }

   /**
    * Sets the prefix for the name of the table where the data will be stored. "_<cache name>" will be appended
    * to this prefix in order to enforce unique table names for each cache.
    */
   public void setBucketTableNamePrefix(String bucketTableName) {
      testImmutability("tableManipulation");
      this.tableManipulation.setTableNamePrefix(bucketTableName);
   }

   public void setTableNamePrefix(String tableNamePrefix) {
      testImmutability("tableManipulation");
      this.tableManipulation.setTableNamePrefix(tableNamePrefix);
   }

}
