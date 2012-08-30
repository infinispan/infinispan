/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other
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

package org.infinispan.loaders.mongodb.configuration;

import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.LoadersConfigurationBuilder;
import org.infinispan.util.TypedProperties;

/**
 * MongoDBCacheStoreConfigurationBuilder. Configures a {@link MongoDBCacheStoreConfiguration}
 * @author Guillaume Scheibel <guillaume.scheibel@gmail.com>
 */
public class MongoDBCacheStoreConfigurationBuilder extends AbstractStoreConfigurationBuilder<MongoDBCacheStoreConfiguration, MongoDBCacheStoreConfigurationBuilder> {

   private String host;
   private int port;
   private int timeout;
   private String username;
   private String password;
   private String database;
   private String collection;
   private int acknowledgment;

   public MongoDBCacheStoreConfigurationBuilder(LoadersConfigurationBuilder builder) {
      super(builder);
      super.fetchPersistentState = true;
      super.purgeOnStartup = true;
      super.ignoreModifications = true;
      super.async.enable();
   }

   @Override
   public MongoDBCacheStoreConfiguration create() {
      return new MongoDBCacheStoreConfiguration(host, port, timeout, username, password, database, collection, acknowledgment,
                                                purgeOnStartup, purgeSynchronously, purgerThreads, fetchPersistentState,
                                                ignoreModifications, TypedProperties.toTypedProperties(properties), async.create(), singletonStore.create());
   }

   @Override
   public MongoDBCacheStoreConfigurationBuilder read(MongoDBCacheStoreConfiguration template) {
      this.host = template.host();
      this.username = template.username();
      this.password = template.password();
      this.database = template.database();
      this.collection = template.collection();
      this.timeout = template.timeout();
      this.acknowledgment = template.acknowledgment();
      super.purgeOnStartup = template.purgeOnStartup();
      super.purgeSynchronously = template.purgeSynchronously();
      super.purgerThreads = template.purgerThreads();
      super.fetchPersistentState = template.fetchPersistentState();
      super.ignoreModifications = template.ignoreModifications();
      return this;
   }

   @Override
   public MongoDBCacheStoreConfigurationBuilder self() {
      return this;
   }

   public MongoDBCacheStoreConfigurationBuilder host(String host) {
      this.host = host;
      return this;
   }

   public MongoDBCacheStoreConfigurationBuilder port(int port) {
      this.port = port;
      return this;
   }

   public MongoDBCacheStoreConfigurationBuilder timeout(int timeout) {
      this.timeout = timeout;
      return this;
   }

   public MongoDBCacheStoreConfigurationBuilder username(String username) {
      this.username = username;
      return this;
   }

   public MongoDBCacheStoreConfigurationBuilder password(String password) {
      this.password = password;
      return this;
   }

   public MongoDBCacheStoreConfigurationBuilder database(String database) {
      this.database = database;
      return this;
   }

   public MongoDBCacheStoreConfigurationBuilder collection(String collection) {
      this.collection = collection;
      return this;
   }

   public MongoDBCacheStoreConfigurationBuilder acknowledgment(int acknowledgment) {
      this.acknowledgment = acknowledgment;
      return this;
   }
}
