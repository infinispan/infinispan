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
package org.infinispan.distribution;

import org.infinispan.CacheException;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.control.RehashControlCommand;
import org.infinispan.config.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.distribution.ch.ConsistentHashHelper;
import org.infinispan.remoting.InboundInvocationHandler;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.infinispan.distribution.ch.ConsistentHashHelper.createConsistentHash;

/**
 * This task is kicked off whenever a MERGE is detected
 *
 * @author Manik Surtani
 * @since 4.2.1
 */
public class MergeTask extends JoinTask {


   public MergeTask(RpcManager rpcManager, CommandsFactory commandsFactory, Configuration conf,
            DataContainer dataContainer, DistributionManagerImpl dmi, InboundInvocationHandler inboundInvocationHandler,
            List<Address> newView, List<List<Address>> mergedGroups) {

      super(rpcManager, commandsFactory, conf, dataContainer, dmi, inboundInvocationHandler);

      chNew = ConsistentHashHelper.createConsistentHash(configuration, newView);

      if (mergedGroups.size() < 2) throw new IllegalArgumentException("Don't know how to handle a merge of " + mergedGroups.size() + " partitions!");
      if (mergedGroups.size() > 2) log.warn("Attempting to merge more than 2 partitions!  Inconsistencies may occur!  See https://issues.jboss.org/browse/ISPN-1001");

      List<Address> a1 = mergedGroups.get(0);
      List<Address> a2 = mergedGroups.get(1);

      if (!a1.contains(self)) chOld = createConsistentHash(configuration, a1);
      else if (!a2.contains(self)) chOld = createConsistentHash(configuration, a2);
      else throw new IllegalArgumentException("Both of the merged partitions " +a1+ " and " + a2 + " contains " + self);
   }

   @Override
   protected void getPermissionToJoin() {
      // don't need to "ask" for permission in this case
   }

   @Override
   protected void broadcastNewConsistentHash() {
      // this is no longer necessary; a no-op
   }

   @Override
   protected void signalJoinRehashEnd() {
      // this is no longer necessary; a no-op
   }
}
