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
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.remote.SingleRpcCommand;
import org.infinispan.container.versioning.EntryVersionsMap;

/**
 * The default response generator for most cache modes
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class DefaultResponseGenerator implements ResponseGenerator {
   @Override
   public Response getResponse(CacheRpcCommand command, Object returnValue) {
      if (command instanceof SingleRpcCommand) {
         //https://issues.jboss.org/browse/ISPN-1984
         SingleRpcCommand src = (SingleRpcCommand) command;
         ReplicableCommand c = src.getCommand();
         if (c.getCommandId()== DistributedExecuteCommand.COMMAND_ID) {
            // Even null values should be wrapped in this case.
            return new SuccessfulResponse(returnValue);
         }
      }
      if (returnValue == null) return null;
      if (returnValue instanceof EntryVersionsMap || command.isReturnValueExpected()) {
         return SuccessfulResponse.create(returnValue);
      } else {
         return null; // saves on serializing a response!
      }
   }
}
