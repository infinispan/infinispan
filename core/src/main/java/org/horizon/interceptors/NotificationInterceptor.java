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

import org.horizon.commands.tx.CommitCommand;
import org.horizon.commands.tx.PrepareCommand;
import org.horizon.commands.tx.RollbackCommand;
import org.horizon.context.InvocationContext;
import org.horizon.factories.annotations.Inject;
import org.horizon.notifications.cachelistener.CacheNotifier;

/**
 * The interceptor in charge of firing off notifications to cache listeners
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani</a>
 * @since 4.0
 */
public class NotificationInterceptor extends BaseTransactionalContextInterceptor {
   private CacheNotifier notifier;

   @Inject
   public void injectDependencies(CacheNotifier notifier) {
      this.notifier = notifier;
   }

   @Override
   public Object visitPrepareCommand(InvocationContext ctx, PrepareCommand command) throws Throwable {
      Object retval = invokeNextInterceptor(ctx, command);
      if (command.isOnePhaseCommit()) notifier.notifyTransactionCompleted(ctx.getTransaction(), true, ctx);

      return retval;
   }

   @Override
   public Object visitCommitCommand(InvocationContext ctx, CommitCommand command) throws Throwable {
      Object retval = invokeNextInterceptor(ctx, command);
      notifier.notifyTransactionCompleted(ctx.getTransaction(), true, ctx);
      return retval;
   }

   @Override
   public Object visitRollbackCommand(InvocationContext ctx, RollbackCommand command) throws Throwable {
      Object retval = invokeNextInterceptor(ctx, command);
      notifier.notifyTransactionCompleted(ctx.getTransaction(), false, ctx);
      return retval;
   }
}
