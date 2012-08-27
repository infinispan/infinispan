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
package org.infinispan.loaders.jdbc.configuration;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.loaders.jdbc.binary.JdbcBinaryCacheStoreConfig;
import org.infinispan.loaders.jdbc.connectionfactory.PooledConnectionFactory;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "loaders.jdbc.configuration.ConfigurationTest")
public class ConfigurationTest {

   public void testJdbcBinaryCacheStoreConfigurationAdaptor() {
      ConfigurationBuilder b = new ConfigurationBuilder();
      b.loaders().addStore(JdbcBinaryCacheStoreConfigurationBuilder.class)
         .connectionUrl("jdbc:h2:mem:infinispan;DB_CLOSE_DELAY=-1")
         .connectionFactoryClass(PooledConnectionFactory.class)
         .table()
            .idColumnName("id").idColumnType("VARCHAR")
            .dataColumnName("datum").dataColumnType("BINARY")
            .timestampColumnName("version").timestampColumnType("BIGINT");
      Configuration configuration = b.build();
      JdbcBinaryCacheStoreConfiguration store = (JdbcBinaryCacheStoreConfiguration) configuration.loaders().cacheLoaders().get(0);

      JdbcBinaryCacheStoreConfig cacheStoreConfig = store.adapt();
      assert cacheStoreConfig.getConnectionFactoryConfig().getConnectionUrl().equals("jdbc:h2:mem:infinispan;DB_CLOSE_DELAY=-1");
   }
}