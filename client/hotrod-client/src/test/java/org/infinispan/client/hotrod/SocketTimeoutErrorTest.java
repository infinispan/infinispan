/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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

package org.infinispan.client.hotrod;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.test.TestingUtil.*;

import java.lang.reflect.Method;
import java.net.SocketTimeoutException;
import java.util.Properties;

import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.EntryWrappingInterceptor;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.Marshaller;
import org.infinispan.marshall.jboss.JBossMarshaller;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.ByteArrayKey;
import org.testng.annotations.Test;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

/**
 * Tests the behaviour of the client upon a socket timeout exception
 * and any invocation after that.
 *
 * @author Galder Zamarreño
 * @since 4.2
 */
@Test(groups = "functional", testName = "client.hotrod.SocketTimeoutErrorTest")
public class SocketTimeoutErrorTest extends SingleCacheManagerTest {

   protected HotRodServer hotrodServer;
   protected RemoteCacheManager remoteCacheManager;
   protected RemoteCache remoteCache;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.customInterceptors().addInterceptor().interceptor(
            new TimeoutInducingInterceptor()).after(EntryWrappingInterceptor.class);
      return TestCacheManagerFactory.createCacheManager(hotRodCacheConfiguration(builder));
   }

   @Override
   protected void setup() throws Exception {
      super.setup();
      hotrodServer = TestHelper.startHotRodServer(cacheManager);
      remoteCacheManager = new RemoteCacheManager(getClientProperties());
      remoteCache = remoteCacheManager.getCache();
   }

   @Override
   protected void teardown() {
      killRemoteCacheManager(remoteCacheManager);
      killServers(hotrodServer);
      super.teardown();
   }

   protected Properties getClientProperties() {
      Properties props = new Properties();
      props.put("infinispan.client.hotrod.server_list", "127.0.0.1:" + hotrodServer.getPort());
      props.setProperty(ConfigurationProperties.SO_TIMEOUT, "3000");
      props.setProperty("maxActive", "1");
      props.setProperty("maxTotal", "1");
      props.setProperty("maxIdle", "1");
      return props;
   }

   public void testErrorWhileDoingPut(Method m) throws Exception {
      remoteCache = remoteCacheManager.getCache();

      remoteCache.put(k(m), v(m));
      assert remoteCache.get(k(m)).equals(v(m));

      try {
         remoteCache.put("FailFailFail", "whatever...");
      } catch (HotRodClientException e) {
         // ignore
         assert e.getCause() instanceof SocketTimeoutException;
      }

      // What counts is that socket timeout exception kicks in, operations
      // after that do not add anything to the socket timeout test
   }

   public static class TimeoutInducingInterceptor extends CommandInterceptor {

      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
         if (unmarshall(command.getKey()).equals("FailFailFail")) {
            Thread.sleep(6000);
         }

         return super.visitPutKeyValueCommand(ctx, command);
      }

      private String unmarshall(Object key) throws Exception {
         Marshaller marshaller = new JBossMarshaller();
         return (String) marshaller.objectFromByteBuffer((byte[]) key);
      }
   }

}