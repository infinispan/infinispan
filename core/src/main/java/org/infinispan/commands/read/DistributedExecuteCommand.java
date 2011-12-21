/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;

import org.infinispan.Cache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.context.InvocationContext;
import org.infinispan.distexec.DistributedCallable;
import org.infinispan.lifecycle.ComponentStatus;

/**
 * DistributedExecuteCommand is used to migrate Callable and execute it in remote JVM.
 * 
 * @author Vladimir Blagojevic
 * @author Mircea Markus
 * @since 5.0
 */
public class DistributedExecuteCommand<V> implements VisitableCommand {

   public static final int COMMAND_ID = 19;
   
   private static final long serialVersionUID = -7828117401763700385L;

   protected Cache cache;

   protected Set<Object> keys;

   protected Callable<V> callable;


   public DistributedExecuteCommand(Collection<Object> inputKeys, Callable<V> callable) {
      if (inputKeys == null || inputKeys.isEmpty())
         this.keys = new HashSet<Object>();
      else
         this.keys = new HashSet<Object>(inputKeys);
      this.callable = callable;
   }

   public DistributedExecuteCommand() {
      this(null, null);
   }

   public void init(Cache cache) {
      this.cache = cache;
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitDistributedExecuteCommand(ctx, this);
   }

   @Override
   public boolean shouldInvoke(InvocationContext ctx) {
      return true;
   }

   /**
    * Performs invocation of Callable and returns result
    * 
    * @param ctx
    *           invocation context
    * @return result of Callable invocations
    */
   @Override
   public Object perform(InvocationContext context) throws Throwable {
      Callable<V> callable = getCallable();
      if (callable instanceof DistributedCallable<?, ?, ?>) {
         DistributedCallable<Object, Object, Object> dc = (DistributedCallable<Object, Object, Object>) callable;
         dc.setEnvironment(cache, keys);
      }
      return callable.call();      
   }

   private Callable<V> getCallable() {
      return callable;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      return new Object[] { keys, callable};
   }

   @Override
   public void setParameters(int commandId, Object[] args) {
      if (commandId != COMMAND_ID)
         throw new IllegalStateException("Invalid method id");
      int i = 0;
      this.keys = (Set<Object>) args[i++];
      this.callable = (Callable) args[i++];
   }
   
   @Override
   public boolean equals(Object o) {
      if (this == o) {
         return true;
      }
      if (!(o instanceof DistributedExecuteCommand)) {
         return false;
      }
      if (!super.equals(o)) {
         return false;
      }
      DistributedExecuteCommand<?> that = (DistributedExecuteCommand) o;
      if (keys.equals(that.keys)) {
         return false;
      }
      return true;
   }

   @Override
   public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (keys != null ? keys.hashCode() : 0);
      return result;
   }

   @Override
   public String toString() {
      return "DistributedExecuteCommand{" +
            "cache=" + cache +
            ", keys=" + keys +
            ", callable=" + callable +
            '}';
   }

   @Override
   public boolean ignoreCommandOnStatus(ComponentStatus status) {
      return false;
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }
}
