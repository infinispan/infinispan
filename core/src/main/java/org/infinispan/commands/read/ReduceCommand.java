/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other
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
import java.util.UUID;

import org.infinispan.commands.CancellableCommand;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.distexec.mapreduce.MapReduceManager;
import org.infinispan.distexec.mapreduce.Reducer;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * ReduceCommand is a container to migrate {@link Reducer} to a remote Infinispan node where it will
 * get executed and return the result to an invoking/master node.
 * 
 * @author Vladimir Blagojevic
 * @since 5.2
 */
public class ReduceCommand<KOut, VOut> extends BaseRpcCommand implements CancellableCommand {

   public static final int COMMAND_ID = 31;
   private static final Log log = LogFactory.getLog(ReduceCommand.class);
   private Set<KOut> keys = new HashSet<KOut>();
   private Reducer<KOut, VOut> reducer;
   private String taskId;
   private boolean emitCompositeIntermediateKeys;
   private MapReduceManager mrManager;
   private UUID uuid;

   private ReduceCommand() {
      super(null); // For command id uniqueness test
   }

   public ReduceCommand(String cacheName) {
      super(cacheName);
   }

   public ReduceCommand(String taskId, Reducer<KOut, VOut> reducer, String cacheName,
            Collection<KOut> inputKeys) {
      super(cacheName);
      this.taskId = taskId;
      if (inputKeys != null && !inputKeys.isEmpty()) {
         keys.addAll(inputKeys);
      }
      this.reducer = reducer;
      this.uuid = UUID.randomUUID();
   }

   public void init(MapReduceManager mrManager) {
      this.mrManager = mrManager;
   }

   /**
    * Performs invocation of reduce phase on assigned Infinispan node
    * 
    * @param context
    *           invocation context
    * @return Map of intermediate key value pairs
    */
   @Override
   public Object perform(InvocationContext context) throws Throwable {
      return mrManager.reduce(this);
   }

   public boolean isEmitCompositeIntermediateKeys() {
      return emitCompositeIntermediateKeys;
   }

   public void setEmitCompositeIntermediateKeys(boolean emitCompositeIntermediateKeys) {
      this.emitCompositeIntermediateKeys = emitCompositeIntermediateKeys;
   }

   public Set<KOut> getKeys() {
      return keys;
   }

   public Reducer<KOut, VOut> getReducer() {
      return reducer;
   }

   public String getTaskId() {
      return taskId;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public UUID getUUID() {
      return uuid;
   }

   @Override
   public Object[] getParameters() {
      return new Object[] { taskId, keys, reducer, emitCompositeIntermediateKeys, uuid };
   }

   @SuppressWarnings({ "unchecked", "rawtypes" })
   @Override
   public void setParameters(int commandId, Object[] args) {
      if (commandId != COMMAND_ID)
         throw new IllegalStateException("Invalid method id");
      int i = 0;
      taskId = (String) args[i++];
      keys = (Set<KOut>) args[i++];
      reducer = (Reducer) args[i++];
      emitCompositeIntermediateKeys = (Boolean) args[i++];
      uuid = (UUID) args[i++];
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }

   @SuppressWarnings("rawtypes")
   @Override
   public boolean equals(Object obj) {
      if (this == obj) {
         return true;
      }
      if (obj == null) {
         return false;
      }
      if (!(obj instanceof ReduceCommand)) {
         return false;
      }
      ReduceCommand other = (ReduceCommand) obj;
      if (taskId == null) {
         if (other.taskId != null) {
            return false;
         }
      } else if (!taskId.equals(other.taskId)) {
         return false;
      }
      return true;
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((taskId == null) ? 0 : taskId.hashCode());
      return result;
   }

   @Override
   public String toString() {
      return "ReduceCommand [keys=" + keys + ", taskId=" + taskId + "]";
   }
}