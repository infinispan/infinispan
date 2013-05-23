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
package org.infinispan.replication;

import org.infinispan.AdvancedCache;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcManagerImpl;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

/**
 * Tests for FORCE_ASYNCHRONOUS and FORCE_SYNCHRONOUS flags.
 *
 * @author Tomas Sykora
 */
@Test(testName = "replication.ForceSyncAsyncFlagsTest", groups = "functional")
@CleanupAfterMethod
public class ForceSyncAsyncFlagsTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() {
      ConfigurationBuilder replSync = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      createClusteredCaches(2, "replSync", replSync);
   }

   public void testForceSyncAndAsyncFlagUsage() throws Exception {

      AdvancedCache cache1 = cache(0, "replSync").getAdvancedCache();
      AdvancedCache cache2 = cache(1, "replSync").getAdvancedCache();
      waitForClusterToForm("replSync");

      Transport originalTransport = null;
      RpcManagerImpl rpcManager = null;
      RpcManagerImpl asyncRpcManager = null;
      Map<Address, Response> emptyResponses = Collections.emptyMap();
      try {
         ConfigurationBuilder asyncCache = getDefaultClusteredCacheConfig(CacheMode.REPL_ASYNC, false);
         asyncCache.clustering().async().asyncMarshalling(true);
         defineConfigurationOnAllManagers("asyncCache", asyncCache);
         AdvancedCache asyncCache1 = manager(0).getCache("asyncCache").getAdvancedCache();
         AdvancedCache asyncCache2 = manager(1).getCache("asyncCache").getAdvancedCache();
         waitForClusterToForm("asyncCache");

         // replace the transport with a mock object
         Transport mockTransport = mock(Transport.class);
         Address mockAddressOne = mock(Address.class);
         Address mockAddressTwo = mock(Address.class);

         List<Address> addresses = new LinkedList<Address>();
         addresses.add(mockAddressOne);
         addresses.add(mockAddressTwo);
         when(mockTransport.getAddress()).thenReturn(mockAddressOne);
         when(mockTransport.getMembers()).thenReturn(addresses);

         // this is shared by all caches managed by the cache manager
         originalTransport = TestingUtil.extractGlobalComponent(cache1.getCacheManager(), Transport.class);
         rpcManager = (RpcManagerImpl) TestingUtil.extractComponent(cache1, RpcManager.class);
         rpcManager.setTransport(mockTransport);

         when(
               mockTransport.invokeRemotely((List<Address>) anyObject(),
                                            (CacheRpcCommand) anyObject(), eq(ResponseMode.SYNCHRONOUS), anyLong(),
                                            anyBoolean(), (ResponseFilter) anyObject(), anyBoolean(), anyBoolean())).thenReturn(emptyResponses);

         // check that the replication call was sync
         cache1.put("k", "v");
         verify(mockTransport).invokeRemotely((List<Address>) anyObject(),
                                              (CacheRpcCommand) anyObject(), eq(ResponseMode.SYNCHRONOUS), anyLong(),
                                              anyBoolean(), (ResponseFilter) anyObject(), anyBoolean(), anyBoolean());

         // verify FORCE_ASYNCHRONOUS flag on SYNC cache
         cache1.withFlags(Flag.FORCE_ASYNCHRONOUS).put("k", "v");
         verify(mockTransport).invokeRemotely((List<Address>) anyObject(),
                                              (CacheRpcCommand) anyObject(), eq(ResponseMode.ASYNCHRONOUS_WITH_SYNC_MARSHALLING), anyLong(),
                                              anyBoolean(), (ResponseFilter) anyObject(), anyBoolean(), anyBoolean());


         // resume to test for async
         asyncRpcManager = (RpcManagerImpl) TestingUtil.extractComponent(asyncCache1, RpcManager.class);
         asyncRpcManager.setTransport(mockTransport);

         reset(mockTransport);
         when(mockTransport.getAddress()).thenReturn(mockAddressOne);
         when(mockTransport.getMembers()).thenReturn(addresses);
         when(
               mockTransport.invokeRemotely((List<Address>) anyObject(),
                                            (CacheRpcCommand) anyObject(), eq(ResponseMode.ASYNCHRONOUS), anyLong(),
                                            anyBoolean(), (ResponseFilter) anyObject(), anyBoolean(), anyBoolean())).thenReturn(emptyResponses);

         asyncCache1.put("k", "v");
         verify(mockTransport).invokeRemotely((List<Address>) anyObject(),
                                              (CacheRpcCommand) anyObject(), eq(ResponseMode.ASYNCHRONOUS), anyLong(),
                                              anyBoolean(), (ResponseFilter) anyObject(), anyBoolean(), anyBoolean());

         // verify FORCE_SYNCHRONOUS flag on ASYNC cache
         asyncCache1.withFlags(Flag.FORCE_SYNCHRONOUS).put("k", "v");
         verify(mockTransport).invokeRemotely((List<Address>) anyObject(),
                                              (CacheRpcCommand) anyObject(), eq(ResponseMode.SYNCHRONOUS), anyLong(),
                                              anyBoolean(), (ResponseFilter) anyObject(), anyBoolean(), anyBoolean());
      } finally {
         // replace original transport
         if (rpcManager != null)
            rpcManager.setTransport(originalTransport);
         if (asyncRpcManager != null)
            asyncRpcManager.setTransport(originalTransport);
      }
   }
}

