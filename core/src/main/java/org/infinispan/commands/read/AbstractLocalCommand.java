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
package org.infinispan.commands.read;

import org.infinispan.commands.LocalCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.lifecycle.ComponentStatus;

/**
 * Abstract class
 *
 * @author Manik Surtani
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class AbstractLocalCommand implements LocalCommand {
   private static final Object[] EMPTY_ARRAY = new Object[0];

   public byte getCommandId() {
      return 0;  // no-op
   }

   public Object[] getParameters() {
      return EMPTY_ARRAY;  // no-op
   }

   public void setParameters(int commandId, Object[] parameters) {
      // no-op
   }

   public boolean shouldInvoke(InvocationContext ctx) {
      return false;
   }

   protected boolean noTxModifications(InvocationContext ctx) {
      return !ctx.isInTxScope() || !((TxInvocationContext)ctx).hasModifications();
   }

   public boolean ignoreCommandOnStatus(ComponentStatus status) {
      return false;
   }

   public boolean isReturnValueExpected() {
      return false;
   }
}
