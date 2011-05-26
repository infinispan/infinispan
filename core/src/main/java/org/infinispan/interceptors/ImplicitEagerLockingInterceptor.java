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
package org.infinispan.interceptors;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.write.EvictCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.CommandInterceptor;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;

/**
 * Interceptor in charge of eager, implicit locking of cache keys across cluster within transactional context
 * <p/>
 * <p/>
 * For more details refer to: https://jira.jboss.org/jira/browse/ISPN-70 https://jira.jboss.org/jira/browse/ISPN-48
 *
 * @author <a href="mailto:vblagoje@redhat.com">Vladimir Blagojevic (vblagoje@redhat.com)</a>
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class ImplicitEagerLockingInterceptor extends CommandInterceptor {

   private CommandsFactory cf;

   @Inject
   public void init(CommandsFactory factory) {
      this.cf = factory;
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      if (shouldAcquireRemoteLock(ctx)) {
         lockEagerly(ctx, Collections.singleton(command.getKey()));
      }
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      if (shouldAcquireRemoteLock(ctx)) {
         lockEagerly(ctx, Collections.singleton(command.getKey()));
      }
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command)
         throws Throwable {
      if (shouldAcquireRemoteLock(ctx)) {
         lockEagerly(ctx, Collections.singleton(command.getKey()));
      }
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      if (shouldAcquireRemoteLock(ctx)) {
         lockEagerly(ctx, new HashSet<Object>(command.getMap().keySet()));
      }
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitEvictCommand(InvocationContext ctx, EvictCommand command) throws Throwable {
      if (shouldAcquireRemoteLock(ctx)) {
         lockEagerly(ctx, Collections.singleton(command.getKey()));
      }
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitInvalidateCommand(InvocationContext ctx, InvalidateCommand command)
         throws Throwable {
      if (shouldAcquireRemoteLock(ctx)) {
         lockEagerly(ctx, Collections.singleton(command.getKey()));
      }
      return invokeNextInterceptor(ctx, command);
   }

   private Object lockEagerly(InvocationContext ctx, Collection<Object> keys) throws Throwable {
      LockControlCommand lcc = cf.buildLockControlCommand(keys, true, ctx.getFlags());
      return invokeNextInterceptor(ctx, lcc);
   }

   private boolean shouldAcquireRemoteLock(InvocationContext ctx) {
      return ctx.isInTxScope() && ctx.isOriginLocal() && !ctx.hasFlag(Flag.CACHE_MODE_LOCAL);
   }
}
