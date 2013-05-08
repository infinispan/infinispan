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
package org.infinispan.loaders.jdbc;

import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactoryConfig;
import org.infinispan.loaders.jdbc.connectionfactory.PooledConnectionFactory;
import org.infinispan.test.fwk.UnitTestDatabaseManager;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.util.HashSet;
import java.util.Set;

/**
 * Tester class for {@link org.infinispan.loaders.jdbc.connectionfactory.PooledConnectionFactory}.
 *
 * @author Mircea.Markus@jboss.com
 * @author Tristan Tarrant
 */
@Test(groups = "functional", testName = "loaders.jdbc.PooledConnectionFactoryTest")
public class PooledConnectionFactoryTest {

   private PooledConnectionFactory factory;

   @AfterMethod
   public void destroyFacotry() {
      factory.stop();
   }

   @Test(enabled = false, description = "This test is disabled due to: http://sourceforge.net/tracker/index.php?func=detail&aid=1892195&group_id=25357&atid=383690")
   public void testValuesNoOverrides() throws Exception {
      factory = new PooledConnectionFactory();
      ConnectionFactoryConfig config = UnitTestDatabaseManager.getUniqueConnectionFactoryConfig();
      factory.start(config, Thread.currentThread().getContextClassLoader());
      int hadcodedMaxPoolSize = factory.getPooledDataSource().getMaxPoolSize();
      Set<Connection> connections = new HashSet<Connection>();
      for (int i = 0; i < hadcodedMaxPoolSize; i++) {
         connections.add(factory.getConnection());
      }
      assert connections.size() == hadcodedMaxPoolSize;
      assert factory.getPooledDataSource().getNumBusyConnections() == hadcodedMaxPoolSize;
      for (Connection conn : connections) {
         conn.close();
      }
      long start = System.currentTimeMillis();
      while (System.currentTimeMillis() - start < 2000) {
         if (factory.getPooledDataSource().getNumBusyConnections() == 0)
            break;
      }
      //this must happen eventually
      assert factory.getPooledDataSource().getNumBusyConnections() == 0;
   }

   @Test(expectedExceptions = CacheLoaderException.class)
   public void testNoDriverClassFound() throws Exception {
      factory = new PooledConnectionFactory();
      ConnectionFactoryConfig config = UnitTestDatabaseManager.getBrokenConnectionFactoryConfig();
      factory.start(config, Thread.currentThread().getContextClassLoader());
   }

}
