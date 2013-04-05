/*
 * Copyright 2012 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

package org.infinispan.interceptors;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.container.EntryFactory;
import org.infinispan.container.entries.ImmortalCacheValue;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.TestAddress;
import org.infinispan.distribution.ch.ReplicatedConsistentHash;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.rpc.RpcOptionsBuilder;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.topology.CacheTopology;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.concurrent.locks.LockManager;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests ReplicationInterceptor.
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
@Test(groups = "functional", testName = "interceptors.ReplicationInterceptorTest")
public class ReplicationInterceptorTest {

   public void testRemoteGetForGetKeyValueCommand() throws Throwable {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().cacheMode(CacheMode.REPL_SYNC);

      GlobalConfigurationBuilder gcb = GlobalConfigurationBuilder.defaultClusteredBuilder();
      Configuration configuration = cb.build();

      ReplicationInterceptor replInterceptor = new ReplicationInterceptor();
      CommandInterceptor nextInterceptor = mock(CommandInterceptor.class);
      when(nextInterceptor.visitGetKeyValueCommand(any(InvocationContext.class), any(GetKeyValueCommand.class))).thenReturn(null);
      replInterceptor.setNext(nextInterceptor);

      CommandsFactory commandsFactory = mock(CommandsFactory.class);
      when(commandsFactory.buildClusteredGetCommand(any(Object.class), any(Set.class), anyBoolean(), any(GlobalTransaction.class))).thenAnswer(new Answer<ClusteredGetCommand>() {
         @Override
         public ClusteredGetCommand answer(InvocationOnMock invocation) {
            Object key = invocation.getArguments()[0];
            Set<Flag> flags = (Set<Flag>) invocation.getArguments()[1];
            boolean acquireRemoteLock = (Boolean) invocation.getArguments()[2];
            GlobalTransaction gtx = (GlobalTransaction) invocation.getArguments()[3];
            return new ClusteredGetCommand(key, "cache1", flags, acquireRemoteLock, gtx, null);
         }
      });

      EntryFactory entryFactory = mock(EntryFactory.class);
      DataContainer dataContainer = mock(DataContainer.class);
      LockManager lockManager = mock(LockManager.class);
      StateTransferManager stateTransferManager = mock(StateTransferManager.class);

      TestAddress A = new TestAddress(0, "A");
      TestAddress B = new TestAddress(1, "B");
      List<Address> members1 = new ArrayList<Address>();
      List<Address> members2 = new ArrayList<Address>();
      members1.add(A);
      members2.add(A);
      members2.add(B);
      ReplicatedConsistentHash readCh = new ReplicatedConsistentHash(members1);
      ReplicatedConsistentHash writeCh = new ReplicatedConsistentHash(members2);
      final CacheTopology cacheTopology = new CacheTopology(1, readCh, writeCh);
      when(stateTransferManager.getCacheTopology()).thenAnswer(new Answer<CacheTopology>() {
         @Override
         public CacheTopology answer(InvocationOnMock invocation) {
            return cacheTopology;
         }
      });
      replInterceptor.injectDependencies(commandsFactory, entryFactory, lockManager, dataContainer, stateTransferManager);
      RpcManager rpcManager = mock(RpcManager.class);
      Transport transport = mock(Transport.class);
      when(rpcManager.getAddress()).thenReturn(B);
      when(rpcManager.getTransport()).thenReturn(transport);
      when(transport.getMembers()).thenReturn(members2);
      replInterceptor.inject(rpcManager, null);
      replInterceptor.injectConfiguration(configuration);

      when(rpcManager.invokeRemotely(any(Collection.class), any(ClusteredGetCommand.class), any(RpcOptions.class)))
                 .thenAnswer(new Answer<Map<Address, Response>>() {
         @Override
         public Map<Address, Response> answer(InvocationOnMock invocation) {
            Collection<Address> recipients = (Collection<Address>) invocation.getArguments()[0];
            ClusteredGetCommand clusteredGetCommand = (ClusteredGetCommand) invocation.getArguments()[1];
            if (clusteredGetCommand.getKey().equals("theKey")) {
               Map<Address, Response> results = new HashMap<Address, Response>();
               for (Address recipient : recipients) {
                  results.put(recipient, SuccessfulResponse.create(new ImmortalCacheValue("theValue")));
               }
               return results;
            }
            return Collections.emptyMap();
         }
      });

      when(rpcManager.getRpcOptionsBuilder(any(ResponseMode.class)))
            .thenAnswer(new Answer<RpcOptionsBuilder>() {
               public RpcOptionsBuilder answer(InvocationOnMock invocation) {
                  Object[] args = invocation.getArguments();
                  return new RpcOptionsBuilder(10000, TimeUnit.MILLISECONDS, (ResponseMode) args[0], true);
               }
            });
      when(rpcManager.getRpcOptionsBuilder(any(ResponseMode.class), anyBoolean()))
            .thenAnswer(new Answer<RpcOptionsBuilder>() {
               public RpcOptionsBuilder answer(InvocationOnMock invocation) {
                  Object[] args = invocation.getArguments();
                  return new RpcOptionsBuilder(10000, TimeUnit.MILLISECONDS, (ResponseMode) args[0], (Boolean) args[1]);
               }
            });

      InvocationContext ctx = mock(InvocationContext.class);
      when(ctx.isOriginLocal()).thenReturn(true);
      when(ctx.isInTxScope()).thenReturn(false);

      GetKeyValueCommand getKeyValueCommand = new GetKeyValueCommand("theKey", null);

      Object retVal = replInterceptor.visitGetKeyValueCommand(ctx, getKeyValueCommand);
      assertEquals("theValue", retVal);
   }
}
