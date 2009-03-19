/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2000 - 2008, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
package org.horizon.interceptors;

import org.horizon.commands.read.GetKeyValueCommand;
import org.horizon.commands.write.ClearCommand;
import org.horizon.commands.write.PutKeyValueCommand;
import org.horizon.commands.write.PutMapCommand;
import org.horizon.commands.write.RemoveCommand;
import org.horizon.commands.write.ReplaceCommand;
import org.horizon.context.InvocationContext;
import org.horizon.eviction.EvictionManager;
import org.horizon.eviction.events.EvictionEvent;
import static org.horizon.eviction.events.EvictionEvent.Type.*;
import org.horizon.factories.annotations.Inject;
import org.horizon.interceptors.base.CommandInterceptor;

/**
 * Eviction Interceptor.
 * <p/>
 * This interceptor is used to handle eviction events.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class EvictionInterceptor extends CommandInterceptor {
   protected EvictionManager evictionManager;
   private static final Object CLEAR_CACHE_KEY_CONSTANT = new Object();

   @Inject
   public void setEvictionCacheManager(EvictionManager evictionManager) {
      this.evictionManager = evictionManager;
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      Object retVal = invokeNextInterceptor(ctx, command);
      if (command.isSuccessful() && command.getKey() != null) {
         registerEvictionEvent(command.getKey(), ADD_ENTRY_EVENT);
      }
      return retVal;
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      Object retVal = invokeNextInterceptor(ctx, command);
      if (command.getMap() == null) {
         if (trace) log.trace("Putting null data.");
      } else {
         for (Object key : command.getMap().keySet()) {
            registerEvictionEvent(key, ADD_ENTRY_EVENT);
         }
      }
      return retVal;
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      Object retVal = invokeNextInterceptor(ctx, command);
      if (command.isSuccessful() && command.getKey() != null) {
         registerEvictionEvent(command.getKey(), REMOVE_ENTRY_EVENT);
      }
      return retVal;
   }

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      Object retVal = invokeNextInterceptor(ctx, command);
      if (retVal == null) {
         if (trace) log.trace("No event added. Element does not exist");
      } else {
         registerEvictionEvent(command.getKey(), VISIT_ENTRY_EVENT);
      }
      return retVal;
   }

   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      Object retval = invokeNextInterceptor(ctx, command);
      registerEvictionEvent(command.getKey(), command.isSuccessful() ? ADD_ENTRY_EVENT : VISIT_ENTRY_EVENT);
      return retval;
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      Object retVal = invokeNextInterceptor(ctx, command);
      registerEvictionEvent(CLEAR_CACHE_KEY_CONSTANT, CLEAR_CACHE_EVENT);
      return retVal;
   }

   @SuppressWarnings(value = "unchecked")
   private void registerEvictionEvent(Object key, EvictionEvent.Type type) {
      if (key == null) {
         log.debug("Cannot record a null key on eviction queue for access type {0}", type);
      } else {
         evictionManager.registerEvictionEvent(key, type);
         if (trace) log.trace("Registering event {0} for entry {1}", type, key);
      }
   }
}
