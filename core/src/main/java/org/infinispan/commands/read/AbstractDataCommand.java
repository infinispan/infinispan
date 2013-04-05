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
package org.infinispan.commands.read;

import org.infinispan.commands.AbstractFlagAffectedCommand;
import org.infinispan.commands.DataCommand;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.lifecycle.ComponentStatus;

import java.util.Set;

import static org.infinispan.util.Util.toStr;

/**
 * @author Mircea.Markus@jboss.com
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2011 Red Hat Inc.
 * @since 4.0
 */
public abstract class AbstractDataCommand extends AbstractFlagAffectedCommand implements DataCommand {
   protected Object key;

   @Override
   public Object getKey() {
      return key;
   }

   public void setKey(Object key) {
      this.key = key;
   }

   protected AbstractDataCommand(Object key, Set<Flag> flags) {
      this.key = key;
      this.flags = flags;
   }

   protected AbstractDataCommand() {
   }

   @Override
   public abstract void setParameters(int commandId, Object[] parameters);

   @Override
   public abstract Object[] getParameters();

   @Override
   public boolean shouldInvoke(InvocationContext ctx) {
      return true;
   }

   @Override
   public boolean ignoreCommandOnStatus(ComponentStatus status) {
      return false;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      AbstractDataCommand other = (AbstractDataCommand) obj;
      if (key == null) {
         if (other.key != null)
            return false;
      } else if (!key.equals(other.key))
         return false;
      if (flags == null) {
         if (other.flags != null)
            return false;
      } else if (!flags.equals(other.flags))
         return false;
      return true;
   }
   
   @Override
   public int hashCode() {
      return (key != null ? key.hashCode() : 0);
   }
   
   @Override
   public String toString() {
      return new StringBuilder(getClass().getSimpleName())
         .append(" {key=")
         .append(toStr(key))
         .append(", flags=").append(flags)
         .append("}")
         .toString();
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }

   @Override
   public boolean canBlock() {
      return false;
   }
}
