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
package org.horizon.commands.read;

import org.horizon.commands.Visitor;
import org.horizon.container.entries.CacheEntry;
import org.horizon.context.InvocationContext;
import org.horizon.logging.Log;
import org.horizon.logging.LogFactory;
import org.horizon.notifications.cachelistener.CacheNotifier;

/**
 * // TODO: MANIK: Document this
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
public class GetKeyValueCommand extends AbstractDataCommand {
   public static final byte METHOD_ID = 26;
   private static final Log log = LogFactory.getLog(GetKeyValueCommand.class);
   private static final boolean trace = log.isTraceEnabled();
   private CacheNotifier notifier;

   public GetKeyValueCommand(Object key, CacheNotifier notifier) {
      this.key = key;
      this.notifier = notifier;
   }

   public GetKeyValueCommand() {
   }

   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitGetKeyValueCommand(ctx, this);
   }

   public Object perform(InvocationContext ctx) throws Throwable {
      CacheEntry entry = ctx.lookupEntry(key);
      if (entry == null || entry.isNull()) {
         if (trace) log.trace("Entry not found");
         return null;
      }
      if (entry.isRemoved()) {
         if (trace) log.trace("Entry has been deleted and is of type " + entry.getClass().getSimpleName());
         return null;
      }
      notifier.notifyCacheEntryVisited(key, true, ctx);
      Object result = entry.getValue();
      if (trace) log.trace("Found value " + result);
      notifier.notifyCacheEntryVisited(key, false, ctx);
      return result;
   }

   public byte getCommandId() {
      return METHOD_ID;
   }
}
