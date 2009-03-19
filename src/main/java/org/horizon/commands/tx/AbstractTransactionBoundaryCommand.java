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

import org.horizon.commands.TransactionBoundaryCommand;
import org.horizon.context.InvocationContext;
import org.horizon.transaction.GlobalTransaction;

/**
 * // TODO: MANIK: Document this
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 1.0
 */
public abstract class AbstractTransactionBoundaryCommand implements TransactionBoundaryCommand {
   GlobalTransaction gtx;

   public GlobalTransaction getGlobalTransaction() {
      return gtx;
   }

   public void setGlobalTransaction(GlobalTransaction gtx) {
      this.gtx = gtx;
   }

   public Object perform(InvocationContext ctx) throws Throwable {
      return null;
   }

   public Object[] getParameters() {
      return new Object[]{gtx};
   }

   public void setParameters(int commandId, Object[] args) {
      gtx = (GlobalTransaction) args[0];
   }

   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      AbstractTransactionBoundaryCommand that = (AbstractTransactionBoundaryCommand) o;

      if (gtx != null ? !gtx.equals(that.gtx) : that.gtx != null) return false;

      return true;
   }

   public int hashCode() {
      return (gtx != null ? gtx.hashCode() : 0);
   }
}
