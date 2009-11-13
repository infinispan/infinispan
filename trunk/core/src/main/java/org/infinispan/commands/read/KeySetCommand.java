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
package org.infinispan.commands.read;

import org.infinispan.commands.LocalCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.container.DataContainer;
import org.infinispan.context.InvocationContext;
import org.infinispan.util.Immutables;

import java.util.Set;

/**
 * KeySetCommand.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
public class KeySetCommand implements VisitableCommand, LocalCommand {
   private final DataContainer container;

   public KeySetCommand(DataContainer container) {
      this.container = container;
   }

   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitKeySetCommand(ctx, this);
   }

   public Set perform(InvocationContext ctx) throws Throwable {
      return Immutables.immutableSetWrap(container.keySet());
   }

   public byte getCommandId() {
      return 0;  // no-op
   }

   public Object[] getParameters() {
      return new Object[0];  // no-op
   }

   public void setParameters(int commandId, Object[] parameters) {
      // no-op
   }

   @Override
   public String toString() {
      return "KeySetCommand{" +
            "set=" + container.keySet() +
            '}';
   }

}
