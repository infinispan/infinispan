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
package org.infinispan.commands.read;

import org.infinispan.commands.DataCommand;
import org.infinispan.context.InvocationContext;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public abstract class AbstractDataCommand implements DataCommand {
   protected Object key;

   public Object getKey() {
      return key;
   }

   public void setKey(Object key) {
      this.key = key;
   }

   protected AbstractDataCommand(Object key) {
      this.key = key;
   }

   protected AbstractDataCommand() {
   }

   public void setParameters(int commandId, Object[] parameters) {
      if (commandId != getCommandId()) throw new IllegalStateException("Invalid method id");
      key = parameters[0];
   }

   public Object[] getParameters() {
      return new Object[]{key};
   }

   public boolean shouldInvoke(InvocationContext ctx) {
      return true;
   }

   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      AbstractDataCommand that = (AbstractDataCommand) o;

      if (key != null ? !key.equals(that.key) : that.key != null) return false;

      return true;
   }

   public int hashCode() {
      return (key != null ? key.hashCode() : 0);
   }

   public String toString() {
      return getClass().getSimpleName() + "{" +
            "key=" + key +
            '}';
   }
}
