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
package org.infinispan.remoting;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcManagerImpl;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "remoting.SingleClusterMemberTransportInvokeTest")
public class SingleClusterMemberTransportInvokeTest extends MultipleCacheManagersTest {
   private final String key = "k", value = "v", value2 = "v2";

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder c = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      createClusteredCaches(1, "replSync", c);
   }

   public void testInvokeRemotelyWhenSingleMember() throws Exception {
      Cache cache1 = cache(0, "replSync");
      Transport mockTransport = mock(Transport.class);
      RpcManagerImpl rpcManager = (RpcManagerImpl) TestingUtil.extractComponent(cache1, RpcManager.class);
      Transport originalTransport = TestingUtil.extractComponent(cache1, Transport.class);
      try {
         Address mockAddress1 = mock(Address.class);
         List<Address> memberList = new ArrayList<Address>(1);
         memberList.add(mockAddress1);
         when(mockTransport.getMembers()).thenReturn(memberList);
         when(mockTransport.getAddress()).thenReturn(null);
         rpcManager.setTransport(mockTransport);
         // Transport invoke remote should not be called.
         // now try a simple replication.  Since the RpcManager is a mock object it will not actually replicate anything.
         cache1.put(key, value);
      } finally {
         if (rpcManager != null) rpcManager.setTransport(originalTransport);
      }
   }
}
