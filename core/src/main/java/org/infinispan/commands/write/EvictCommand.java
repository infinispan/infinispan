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
package org.infinispan.commands.write;

import org.infinispan.commands.LocalCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.context.InvocationContext;
import org.infinispan.notifications.cachelistener.CacheNotifier;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class EvictCommand extends RemoveCommand implements LocalCommand {

   public EvictCommand(Object key, CacheNotifier notifier) {
      this.key = key;
      this.notifier = notifier;
   }

   public void initialize(CacheNotifier notifier) {
      this.notifier = notifier;
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitEvictCommand(ctx, this);
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      if (key == null) {
         throw new NullPointerException("Key is null!!");
      }
      super.perform(ctx);
      return null;
   }

   @Override
   public void notify(InvocationContext ctx, Object value, boolean isPre) {
      notifier.notifyCacheEntryEvicted(key, value, isPre, ctx);
   }

   @Override
   public byte getCommandId() {
      return -1; // these are not meant for replication!
   }
}
