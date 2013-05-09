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
package org.infinispan.remoting.responses;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.read.DistributedExecuteCommand;
import org.infinispan.commands.read.MapCombineCommand;
import org.infinispan.commands.read.ReduceCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.remote.SingleRpcCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;

/**
 * A response generator for the DIST cache mode
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class DistributionResponseGenerator implements ResponseGenerator {
   DistributionManager distributionManager;

   @Inject
   public void inject(DistributionManager distributionManager) {
      this.distributionManager = distributionManager;
   }

   @Override
   public Response getResponse(CacheRpcCommand command, Object returnValue) {
      if (command.getCommandId() == ClusteredGetCommand.COMMAND_ID) {
         if (returnValue == null) return null;
         ClusteredGetCommand clusteredGet = (ClusteredGetCommand) command;
         if (distributionManager.isAffectedByRehash(clusteredGet.getKey()))
            return UnsureResponse.INSTANCE;
         return SuccessfulResponse.create(returnValue);
      } else if (command instanceof SingleRpcCommand) {
         SingleRpcCommand src = (SingleRpcCommand) command;
         ReplicableCommand c = src.getCommand();
         byte commandId = c.getCommandId();
         if (c instanceof WriteCommand) {
            if (returnValue == null) return null;
            // check if this is successful.
            WriteCommand wc = (WriteCommand) c;
            return handleWriteCommand(wc, returnValue);
         } else if (commandId == MapCombineCommand.COMMAND_ID ||
                  commandId == ReduceCommand.COMMAND_ID ||
                  commandId == DistributedExecuteCommand.COMMAND_ID) {
            // Even null values should be wrapped in this case.
            return SuccessfulResponse.create(returnValue);
         } else if (c.isReturnValueExpected()) {
            if (returnValue == null) return null;
            return SuccessfulResponse.create(returnValue);
         }
      } else if (command.isReturnValueExpected()) {
         return SuccessfulResponse.create(returnValue);
      }
      return null; // no unnecessary response values!
   }

   protected Response handleWriteCommand(WriteCommand wc, Object returnValue) {
      return wc.isReturnValueExpected() ? SuccessfulResponse.create(returnValue) : null;
   }
}
