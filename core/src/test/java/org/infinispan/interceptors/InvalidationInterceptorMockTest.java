/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
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
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.junit.Test;
import org.mockito.Matchers;

import java.util.Collections;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test mainly for fix of https://bugzilla.redhat.com/show_bug.cgi?id=1373412
 */
public class InvalidationInterceptorMockTest {

    @Test
    public void testInvalidateAcrossCluster() throws Throwable {

        testInvalidateAcrossCluster(true);
        testInvalidateAcrossCluster(false);
    }

    private void testInvalidateAcrossCluster(boolean inTransaction) throws Throwable {
        InvalidationInterceptor invalidationInterceptor = new InvalidationInterceptor();

        InvalidateCommand invalidateCommand = mock(InvalidateCommand.class);
        PrepareCommand prepareCommand = mock(PrepareCommand.class);
        CommandsFactory commandsFactory = mock(CommandsFactory.class);
        when(commandsFactory.buildInvalidateCommand(anySet(), Matchers.anyObject())).thenReturn(invalidateCommand);
        when(commandsFactory.buildPrepareCommand(any(GlobalTransaction.class), anyList(), any(Boolean.class))).thenReturn(prepareCommand);

        InvocationContext ctx = (inTransaction ? mock(TxInvocationContext.class) : mock(InvocationContext.class));

        when(ctx.isInTxScope()).thenReturn(inTransaction);

        RpcManager rpcManager = mock(RpcManager.class);
        when(rpcManager.getTransport()).thenReturn(mock(Transport.class));

        invalidationInterceptor.inject(rpcManager);
        invalidationInterceptor.injectDependencies(commandsFactory);

        invalidationInterceptor.invalidateAcrossCluster(true, null, ctx);

        if (inTransaction) {
            verify(rpcManager, times(1)).broadcastRpcCommand(prepareCommand, true);
        } else {
            verify(rpcManager, times(1)).broadcastRpcCommand(invalidateCommand, true);
        }
    }
}
