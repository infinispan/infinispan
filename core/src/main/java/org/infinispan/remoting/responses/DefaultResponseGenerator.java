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

import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.remote.recovery.CompleteTransactionCommand;
import org.infinispan.commands.remote.recovery.GetInDoubtTransactionsCommand;
import org.infinispan.commands.remote.recovery.GetInDoubtTxInfoCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.container.versioning.EntryVersionsMap;

/**
 * The default response generator for most cache modes
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class DefaultResponseGenerator implements ResponseGenerator {
   public Response getResponse(CacheRpcCommand command, Object returnValue) {
      if (returnValue == null) return null;
      if (requiresResponse(command.getCommandId(), returnValue)) {
         return new SuccessfulResponse(returnValue);
      } else {
         return null; // saves on serializing a response!
      }
   }

   private boolean requiresResponse(byte commandId, Object rv) {
      boolean commandRequiresResp = commandId == ClusteredGetCommand.COMMAND_ID || commandId == GetInDoubtTransactionsCommand.COMMAND_ID
            || commandId == GetInDoubtTxInfoCommand.COMMAND_ID || commandId == CompleteTransactionCommand.COMMAND_ID
            || commandId == CommitCommand.COMMAND_ID;

      return commandRequiresResp || rv instanceof EntryVersionsMap;
   }
}
