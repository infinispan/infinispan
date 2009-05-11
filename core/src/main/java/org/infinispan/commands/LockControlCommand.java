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
package org.infinispan.commands;

import java.util.Collection;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.context.InvocationContext;

/**
 * 
 *
 * @author Vladimir Blagojevic (<a href="mailto:vblagoje@redhat.com">vblagoje@redhat.com</a>)
 * @param 
 * @since 4.0
 */
public class LockControlCommand implements VisitableCommand {
   private final Collection keys;
   private final boolean lock;

   public LockControlCommand(Collection keys,boolean lock) {
      this.keys=keys;
      this.lock = lock;
   }

   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitLockControlCommand(ctx, this);
   }

   public Boolean perform(InvocationContext ctx) throws Throwable {
      return true;
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
      return "LockControlCommand{" +
            "lock=" + lock +
            "keys=" + keys +
            '}';
   }
}
