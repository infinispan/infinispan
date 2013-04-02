/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
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
package org.infinispan.client.hotrod;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.configuration.ExhaustedAction;
import org.testng.annotations.Test;

@Test(testName = "client.hotrod.ConfigurationTest", groups = "functional" )
public class ConfigurationTest {

   public void testConfiguration() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder
         .addServer()
            .host("host1")
            .port(11222)
         .addServer()
            .host("host2")
            .port(11222)
         .asyncExecutorFactory()
            .factoryClass(SomeAsyncExecutorFactory.class)
         .balancingStrategy(SomeRequestBalancingStrategy.class)
         .connectionPool()
            .maxActive(100)
            .maxTotal(150)
            .maxWait(1000)
            .maxIdle(20)
            .minIdle(10)
            .exhaustedAction(ExhaustedAction.WAIT)
            .numTestsPerEvictionRun(5)
            .testOnBorrow(true)
            .testOnReturn(true)
            .testWhileIdle(false)
            .minEvictableIdleTime(12000)
            .timeBetweenEvictionRuns(15000)
         .connectionTimeout(100)
         .consistentHashImpl(1, SomeCustomConsistentHashV1.class)
         .socketTimeout(100)
         .tcpNoDelay(false)
         .pingOnStartup(false)
         .keySizeEstimate(128)
         .valueSizeEstimate(1024)
         .transportFactory(SomeTransportfactory.class);

      Configuration configuration = builder.build();
      validateConfiguration(configuration);

      ConfigurationBuilder newBuilder = new ConfigurationBuilder();
      newBuilder.read(configuration);
      Configuration newConfiguration = newBuilder.build();
      validateConfiguration(newConfiguration);
   }

   private void validateConfiguration(Configuration configuration) {
      assertEquals(2, configuration.servers().size());
      assertEquals(SomeAsyncExecutorFactory.class, configuration.asyncExecutorFactory().factoryClass());
      assertEquals(SomeRequestBalancingStrategy.class, configuration.balancingStrategy());
      assertEquals(SomeTransportfactory.class, configuration.transportFactory());
      assertEquals(SomeCustomConsistentHashV1.class, configuration.consistentHashImpl(1));
      assertEquals(100, configuration.connectionPool().maxActive());
      assertEquals(150, configuration.connectionPool().maxTotal());
      assertEquals(1000, configuration.connectionPool().maxWait());
      assertEquals(20, configuration.connectionPool().maxIdle());
      assertEquals(10, configuration.connectionPool().minIdle());
      assertEquals(ExhaustedAction.WAIT, configuration.connectionPool().exhaustedAction());
      assertEquals(5, configuration.connectionPool().numTestsPerEvictionRun());
      assertEquals(15000, configuration.connectionPool().timeBetweenEvictionRuns());
      assertEquals(12000, configuration.connectionPool().minEvictableIdleTime());
      assertTrue(configuration.connectionPool().testOnBorrow());
      assertTrue(configuration.connectionPool().testOnReturn());
      assertFalse(configuration.connectionPool().testWhileIdle());
      assertEquals(100, configuration.connectionTimeout());
      assertEquals(100, configuration.socketTimeout());
      assertFalse(configuration.tcpNoDelay());
      assertFalse(configuration.pingOnStartup());
      assertEquals(128, configuration.keySizeEstimate());
      assertEquals(1024, configuration.valueSizeEstimate());
   }
}
