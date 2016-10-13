/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other
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

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;

/**
 * @author Guillaume Scheibel <guillaume.scheibel@gmail.com>
 */
@Test(groups = "unit", testName = "loaders.mongodb.configuration.ConfigurationTest")
public class ConfigurationTest {

   public void testMongoDBConfigurationBuilder() {
      final String host = "localhost";
      final int port = 27017;
      final int timeout = 1500;
      final String username = "mongoDBUSer";
      final String password = "mongoBDPassword";
      final String database = "infinispan_cachestore";
      final String collection = "entries";

      ConfigurationBuilder b = new ConfigurationBuilder();
      b.loaders().addStore(MongoDBCacheStoreConfigurationBuilder.class)
            .host(host)
            .port(port)
            .timeout(timeout)
            .acknowledgment(0)
            .username(username)
            .password(password)
            .database(database)
            .collection(collection);

      final Configuration config = b.build();
      MongoDBCacheStoreConfiguration store = (MongoDBCacheStoreConfiguration) config.loaders().cacheLoaders().get(0);

      assertEquals(store.host(), host);
      assertEquals(store.port(), port);
      assertEquals(store.timeout(), timeout);
      assertEquals(store.username(), username);
      assertEquals(store.password(), password);
      assertEquals(store.database(), database);
      assertEquals(store.collection(), collection);
   }
}
