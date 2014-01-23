/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.tx.dld;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.SingleRpcCommand;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.util.concurrent.NotifyingNotifiableFuture;
import org.infinispan.util.AbstractControlledRpcManager;
import org.infinispan.util.concurrent.ReclosableLatch;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
public class ControlledRpcManager extends AbstractControlledRpcManager {

   private final ReclosableLatch replicationLatch = new ReclosableLatch(true);
   private final ReclosableLatch blockingLatch = new ReclosableLatch(true);
   private volatile Set<Class> blockBeforeFilter = Collections.emptySet();
   private volatile Set<Class> blockAfterFilter = Collections.emptySet();
   private volatile Set<Class> failFilter = Collections.emptySet();

   public ControlledRpcManager(RpcManager realOne) {
      super(realOne);
   }

   public void failFor(Class... filter) {
      this.failFilter = new HashSet<Class>(Arrays.asList(filter));
      blockingLatch.open();
   }

   public void stopFailing() {
      this.failFilter = Collections.emptySet();
      blockingLatch.open();
   }

   public void blockBefore(Class... filter) {
      this.blockBeforeFilter = new HashSet<Class>(Arrays.asList(filter));
      replicationLatch.close();
      blockingLatch.close();
   }

   public void blockAfter(Class... filter) {
      this.blockAfterFilter = new HashSet<Class>(Arrays.asList(filter));
      replicationLatch.close();
      blockingLatch.close();
   }

   public void stopBlocking() {
      log.tracef("Stop blocking commands");
      blockBeforeFilter = Collections.emptySet();
      blockAfterFilter = Collections.emptySet();
      replicationLatch.open();
   }

   public void waitForCommandToBlock() throws InterruptedException {
      log.tracef("Waiting for at least one command to block");
      blockingLatch.await();
   }

   public boolean waitForCommandToBlock(long time, TimeUnit unit) throws InterruptedException {
      return blockingLatch.await(time, unit);
   }

   public void failIfNeeded(ReplicableCommand rpcCommand) {
      if (failFilter.contains(getActualClass(rpcCommand))) {
         throw new IllegalStateException("Induced failure!");
      }
   }

   protected void waitBefore(ReplicableCommand rpcCommand) {
      waitForReplicationLatch(rpcCommand, blockBeforeFilter);
   }

   protected void waitAfter(ReplicableCommand rpcCommand) {
      waitForReplicationLatch(rpcCommand, blockAfterFilter);
   }

   protected void waitForReplicationLatch(ReplicableCommand rpcCommand, Set<Class> filter) {
      Class cmdClass = getActualClass(rpcCommand);
      if (!filter.contains(cmdClass)) {
         return;
      }

      try {
         blockingLatch.open();
         log.debugf("Replication trigger called, waiting for latch to open.");
         replicationLatch.await();
         log.trace("Replication latch opened, continuing.");
      } catch (Exception e) {
         throw new RuntimeException("Unexpected exception!", e);
      }
   }

   private Class getActualClass(ReplicableCommand rpcCommand) {
      Class cmdClass = rpcCommand.getClass();
      if (cmdClass.equals(SingleRpcCommand.class)) {
         cmdClass = ((SingleRpcCommand) rpcCommand).getCommand().getClass();
      }
      return cmdClass;
   }

   public Transport getTransport() {
      return realOne.getTransport();
   }

   public Address getAddress() {
      return realOne.getAddress();
   }
   @Override
   public int getTopologyId() {
      return realOne.getTopologyId();
   }

   @Override
   protected void beforeInvokeRemotely(ReplicableCommand command) {
      failIfNeeded(command);
      waitBefore(command);
   }

   @Override
   protected Map<Address, Response> afterInvokeRemotely(ReplicableCommand command, Map<Address, Response> responseMap) {
      waitAfter(command);
      return responseMap;
   }
}
