/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package org.infinispan.remoting.rpc;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.lifecycle.ComponentStatus;

import java.io.Serializable;

/**
 * @author anistor@redhat.com
 * @since 5.3
 */
public class CustomCacheRpcCommand extends BaseRpcCommand implements VisitableCommand, Serializable {

   public static final byte COMMAND_ID = 126;

   private static final long serialVersionUID = -1L;

   private Object arg;

   public CustomCacheRpcCommand() {
      super(null); // For command id uniqueness test
   }

   public CustomCacheRpcCommand(String cacheName) {
      super(cacheName);
   }

   public CustomCacheRpcCommand(String cacheName, Object arg) {
      this(cacheName);
      this.arg = arg;
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      if (arg instanceof Throwable) {
         throw (Throwable) arg;
      }

      // echo the arg back to the caller
      return arg;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      return new Object[]{arg};
   }

   @Override
   public void setParameters(int commandId, Object[] parameters) {
      if (commandId != COMMAND_ID) {
         throw new IllegalArgumentException("This is not the command id we expect: " + commandId);
      }
      arg = parameters[0];
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitUnknownCommand(ctx, this);
   }

   @Override
   public boolean shouldInvoke(InvocationContext ctx) {
      return true;
   }

   @Override
   public boolean ignoreCommandOnStatus(ComponentStatus status) {
      return false;
   }
}
