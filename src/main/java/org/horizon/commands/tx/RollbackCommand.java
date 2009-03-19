/*
 * JBoss, Home of Professional Open Source
 * Copyright 2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
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
package org.horizon.commands.tx;

import org.horizon.commands.Visitor;
import org.horizon.context.InvocationContext;
import org.horizon.transaction.GlobalTransaction;

/**
 * // TODO: MANIK: Document this
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 1.0
 */
public class RollbackCommand extends AbstractTransactionBoundaryCommand {
   public static final byte METHOD_ID = 12;

   public RollbackCommand(GlobalTransaction globalTransaction) {
      this.gtx = globalTransaction;
   }

   public RollbackCommand() {
   }

   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitRollbackCommand(ctx, this);
   }

   public byte getCommandId() {
      return METHOD_ID;
   }

   public String toString() {
      return "RollbackCommand{" +
            "gtx=" + gtx +
            '}';
   }
}
