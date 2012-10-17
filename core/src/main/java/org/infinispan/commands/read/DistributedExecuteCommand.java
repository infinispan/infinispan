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

import org.infinispan.Cache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.context.InvocationContext;
import org.infinispan.distexec.DistributedCallable;
import org.infinispan.distexec.spi.DistributedTaskLifecycleService;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.util.InfinispanCollections;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;

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

   private Cache<Object, Object> cache;

   private Set<Object> keys;

   private Callable<V> callable;


   public DistributedExecuteCommand(Collection<Object> inputKeys, Callable<V> callable) {
      if (inputKeys == null || inputKeys.isEmpty())
         this.keys = InfinispanCollections.emptySet();
      else
         this.keys = new HashSet<Object>(inputKeys);
      this.callable = callable;
   }

   public DistributedExecuteCommand() {
      this(null, null);
   }

   public void init(Cache<Object, Object> cache) {
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
      // hook into lifecycle
      DistributedTaskLifecycleService taskLifecycleService = DistributedTaskLifecycleService.getInstance();
      Callable<V> callable = getCallable();
      V result = null;
      try {
         taskLifecycleService.onPreExecute(callable, cache);
         if (callable instanceof DistributedCallable<?, ?, ?>) {
            DistributedCallable<Object, Object, Object> dc = (DistributedCallable<Object, Object, Object>) callable;
            dc.setEnvironment(cache, keys);
         }
         result = callable.call();
      } finally {
         taskLifecycleService.onPostExecute(callable);
      }
      return result;
   }

   public Callable<V> getCallable() {
      return callable;
   }
   
   public Set<Object> getKeys(){
      return keys;
   }
   
   public boolean hasKeys(){
      return keys.isEmpty();
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
      this.callable = (Callable<V>) args[i++];
   }
   
  

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((keys == null) ? 0 : keys.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj) {
         return true;
      }
      if (obj == null) {
         return false;
      }
      if (!(obj instanceof DistributedExecuteCommand)) {
         return false;
      }
      DistributedExecuteCommand other = (DistributedExecuteCommand) obj;
      if (keys == null) {
         if (other.keys != null) {
            return false;
         }
      } else if (!keys.equals(other.keys)) {
         return false;
      }
      return true;
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
