/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2000 - 2011, Red Hat Middleware LLC, and individual contributors
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

package org.infinispan.interceptors;

import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.marshall.NotSerializableException;
import org.infinispan.marshall.StreamingMarshaller;

import java.util.Map;

/**
 * Interceptor to verify whether parameters passed into cache are marshallables
 * or not. This is handy in situations where we want to find out before
 * marshalling whether the type of object is marshallable. Such situations
 * include lazy deserialization, or when marshalling happens in a separate
 * thread and marshalling failures might be swallowed. </p>
 *
 * This interceptor offers the possibility to discover these issues way before
 * the code has moved onto a different thread where it's harder to communicate
 * with the original request thread.
 *
 * @author Galder Zamarreño
 * @since 4.2
 */
public class IsMarshallableInterceptor extends CommandInterceptor {

   private StreamingMarshaller marshaller;
   private DistributionManager distManager;

   @Inject
   protected void injectMarshaller(StreamingMarshaller marshaller, DistributionManager distManager) {
      this.marshaller = marshaller;
      this.distManager = distManager;
   }

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      Object key = command.getKey();
      if (isLazyDeserialization() || getMightGoRemote(ctx, key))
         checkMarshallable(key);
      return super.visitGetKeyValueCommand(ctx, command);
   }

   @Override
   public Object visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command) throws Throwable {
      if (isLazyDeserialization() || isClusterInvocation(ctx))
         checkMarshallable(command.getKeys());
      return super.visitLockControlCommand(ctx, command);
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      if (isLazyDeserialization() || isClusterInvocation(ctx) || isStoreInvocation(ctx))
         checkMarshallable(command.getKey(), command.getValue());
      return super.visitPutKeyValueCommand(ctx, command);
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      if (isLazyDeserialization() || isClusterInvocation(ctx) || isStoreInvocation(ctx))
         checkMarshallable(command.getMap());
      return super.visitPutMapCommand(ctx, command);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      if (isLazyDeserialization() || isClusterInvocation(ctx) || isStoreInvocation(ctx))
         checkMarshallable(command.getKey());
      return super.visitRemoveCommand(ctx, command);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      if (isLazyDeserialization() || isClusterInvocation(ctx) || isStoreInvocation(ctx))
         checkMarshallable(command.getKey(), command.getNewValue());
      return super.visitReplaceCommand(ctx, command);
   }

   private boolean isClusterInvocation(InvocationContext ctx) {
      // If the cache is local, the interceptor should only be enabled in case
      // of lazy deserialization or when an async store is in place. So, if
      // any cache store is configured, check whether it'll be skipped
      return ctx.isOriginLocal()
            && configuration.getCacheMode().isClustered()
            && !ctx.hasFlag(Flag.CACHE_MODE_LOCAL);
   }

   private boolean isStoreInvocation(InvocationContext ctx) {
      // If the cache is local, the interceptor should only be enabled in case
      // of lazy deserialization or when an async store is in place. So, if
      // any cache store is configured, check whether it'll be skipped
      return !configuration.getCacheMode().isClustered()
            && configuration.getCacheLoaderManagerConfig().getFirstCacheLoaderConfig() != null
            && !ctx.hasFlag(Flag.SKIP_CACHE_STORE);
   }

   private boolean isLazyDeserialization() {
      return configuration.isUseLazyDeserialization();
   }

   private boolean getMightGoRemote(InvocationContext ctx, Object key) {
      return ctx.isOriginLocal()
            && configuration.getCacheMode().isDistributed()
            && !ctx.hasFlag(Flag.SKIP_REMOTE_LOOKUP)
            && !distManager.getLocality(key).isLocal();
   }

   private void checkMarshallable(Object... objs) throws NotSerializableException {
      for (Object o : objs) {
         if (!marshaller.isMarshallable(o))
            throw new NotSerializableException(String.format(
               "Object of type %s expected to be marshallable", o.getClass()
            ));
      }
   }

   private void checkMarshallable(Map<Object, Object> objs) throws NotSerializableException {
      for (Map.Entry entry : objs.entrySet())
         checkMarshallable(entry.getKey(), entry.getValue());
   }

}
