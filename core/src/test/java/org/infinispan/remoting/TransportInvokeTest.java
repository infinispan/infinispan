/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

import static org.easymock.EasyMock.createNiceMock;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

import java.util.ArrayList;
import java.util.List;

import javax.transaction.TransactionManager;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.ReplListener;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.testng.annotations.Test;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcManagerImpl;

/**
 * TransportInvokeTest.
 * 
 * @author Galder Zamarreño
 * @since 4.0
 */
@Test(groups = "functional", testName = "remoting.TransportInvokeTest")
public class TransportInvokeTest extends MultipleCacheManagersTest {
   final String key = "k", value = "v", value2 = "v2";
   Cache cache1;
   TransactionManager tm1;
   ReplListener replListener1;

   @Override
   protected void createCacheManagers() throws Throwable {
      Configuration c = getDefaultClusteredConfig(Configuration.CacheMode.REPL_SYNC);
      c.setTransactionManagerLookupClass(DummyTransactionManagerLookup.class.getName());
      createClusteredCaches(1, "replSync", c);
      cache1 = cache(0, "replSync");
      tm1 = TestingUtil.getTransactionManager(cache1);
      replListener1 = replListener(cache1);
   }
   
   public void testInvokeRemotelyWhenSingleMember() throws Exception {
      Transport mockTransport = createMock(Transport.class);
      RpcManagerImpl rpcManager = (RpcManagerImpl) TestingUtil.extractComponent(cache1, RpcManager.class);
      Transport originalTransport = TestingUtil.extractComponent(cache1, Transport.class);
      try {

         Address mockAddress1 = createNiceMock(Address.class);
         List<Address> memberList = new ArrayList<Address>(1);
         memberList.add(mockAddress1);
         expect(mockTransport.getMembers()).andReturn(memberList).anyTimes();
         rpcManager.setTransport(mockTransport);
         // Transport invoke remote should not be called.
         replay(mockAddress1, mockTransport);
         // now try a simple replication.  Since the RpcManager is a mock object it will not actually replicate anything.
         cache1.put(key, value);
         verify(mockTransport);

      } finally {
         if (rpcManager != null) rpcManager.setTransport(originalTransport);
      }
   }
}
