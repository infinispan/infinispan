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

import static org.easymock.EasyMock.*;

import java.io.EOFException;

import org.infinispan.Cache;
import org.infinispan.CacheException;
import org.infinispan.commands.remote.SingleRpcCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.config.Configuration;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.CommandAwareRpcDispatcher;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.jgroups.blocks.RpcDispatcher.Marshaller2;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "remoting.TransportSenderExceptionHandlingTest")
public class TransportSenderExceptionHandlingTest extends MultipleCacheManagersTest {
   final String key = "k-illyria", value = "v-illyria", value2 = "v2-illyria";


   @Override
   protected void createCacheManagers() throws Throwable {
      Configuration c = getDefaultClusteredConfig(Configuration.CacheMode.REPL_SYNC);
      c.setTransactionManagerLookupClass(DummyTransactionManagerLookup.class.getName());
      createClusteredCaches(2, "replSync", c);   
   }
   
   public void testInvokeAndExceptionWhileUnmarshalling() throws Exception {
      Cache cache1 = cache(0, "replSync");
      Cache cache2 = cache(1, "replSync");
      JGroupsTransport transport1 = (JGroupsTransport) TestingUtil.extractComponent(cache1, Transport.class);
      CommandAwareRpcDispatcher dispatcher1 = transport1.getCommandAwareRpcDispatcher();
      Marshaller2 originalMarshaller1 = (Marshaller2) dispatcher1.getMarshaller();
      JGroupsTransport transport2 = (JGroupsTransport) TestingUtil.extractComponent(cache2, Transport.class);
      CommandAwareRpcDispatcher dispatcher2 = transport2.getCommandAwareRpcDispatcher();
      Marshaller2 originalMarshaller2 = (Marshaller2) dispatcher2.getMarshaller();
      try {
         Marshaller2 mockMarshaller1 = createMock(Marshaller2.class);
         Marshaller2 mockMarshaller2 = createMock(Marshaller2.class);
         PutKeyValueCommand putCommand = new PutKeyValueCommand();
         putCommand.setKey(key);
         putCommand.setValue(value);
         SingleRpcCommand rpcCommand = new SingleRpcCommand("replSync");
         Object[] params = new Object[]{putCommand};
         rpcCommand.setParameters(SingleRpcCommand.COMMAND_ID, params);
         expect(mockMarshaller1.objectToBuffer(anyObject())).andReturn(originalMarshaller1.objectToBuffer(rpcCommand));
         expect(mockMarshaller2.objectFromByteBuffer((byte[]) anyObject(), anyInt(), anyInt())).andThrow(new EOFException());
         dispatcher1.setRequestMarshaller(mockMarshaller1);
         dispatcher2.setRequestMarshaller(mockMarshaller2);
         replay(mockMarshaller1, mockMarshaller2);
         cache1.put(key, value);
         assert false : "Should have thrown an exception";
      } catch(CacheException ce) {
         assert !(ce.getCause() instanceof ClassCastException) : "No way a ClassCastException must be sent back to user!";
         assert ce.getCause() instanceof EOFException;
      } finally {
         dispatcher1.setMarshaller(originalMarshaller1);
         dispatcher2.setMarshaller(originalMarshaller2);
      }
   }
}
