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
package org.infinispan.commands.control;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.remoting.transport.Transport;

/**
 * A command that informs caches participating in a state transfer of the various stages in the state transfer process.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class StateTransferControlCommand implements ReplicableCommand {
   public static final int COMMAND_ID = 15;
   Transport transport;
   boolean enabled;

   public StateTransferControlCommand() {
   }

   public StateTransferControlCommand(boolean enabled) {
      this.enabled = enabled;
   }

   public void init(Transport transport) {
      this.transport = transport;
   }

   public Object perform(InvocationContext ctx) throws Throwable {
      if (enabled)
         transport.getDistributedSync().acquireSync();
      else
         transport.getDistributedSync().releaseSync();
      return null;
   }

   public byte getCommandId() {
      return COMMAND_ID;
   }

   public Object[] getParameters() {
      return new Object[]{enabled};
   }

   public void setParameters(int commandId, Object[] parameters) {
      enabled = (Boolean) parameters[0];
   }

   @Override
   public String toString() {
      return "StateTransferControlCommand{" +
            "enabled=" + enabled +
            '}';
   }
}
