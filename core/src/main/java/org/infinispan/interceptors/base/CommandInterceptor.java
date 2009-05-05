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
package org.infinispan.interceptors.base;

import org.infinispan.commands.AbstractVisitor;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.config.Configuration;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * This is the base class for all interceptors to extend, and implements the {@link org.infinispan.commands.Visitor}
 * interface allowing it to intercept invocations on {@link org.infinispan.commands.VisitableCommand}s.
 * <p/>
 * Commands are either created by the {@link org.infinispan.CacheDelegate} (for invocations on the {@link
 * org.infinispan.Cache} public interface), or by the {@link org.infinispan.marshall.CommandAwareRpcDispatcher} for
 * remotely originating invocations, and are passed up the interceptor chain by using the {@link
 * org.infinispan.interceptors.InterceptorChain} helper class.
 * <p/>
 * When writing interceptors, authors can either override a specific visitXXX() method (such as {@link
 * #visitGetKeyValueCommand(org.infinispan.context.InvocationContext, org.infinispan.commands.read.GetKeyValueCommand)})
 * or the more generic {@link #handleDefault(org.infinispan.context.InvocationContext,
 * org.infinispan.commands.VisitableCommand)} which is the default behaviour of any visit method, as defined in {@link
 * AbstractVisitor#handleDefault(org.infinispan.context.InvocationContext, org.infinispan.commands.VisitableCommand)}.
 * <p/>
 * The preferred approach is to override the specific visitXXX() methods that are of interest rather than to override
 * {@link #handleDefault(org.infinispan.context.InvocationContext, org.infinispan.commands.VisitableCommand)} and then
 * write a series of if statements or a switch block, if command-specific behaviour is needed.
 * <p/>
 *
 * @author Mircea.Markus@jboss.com
 * @see org.infinispan.commands.VisitableCommand
 * @see org.infinispan.commands.Visitor
 * @see org.infinispan.interceptors.InterceptorChain
 * @since 4.0
 */
@Scope(Scopes.NAMED_CACHE)
public abstract class CommandInterceptor extends AbstractVisitor {
   private CommandInterceptor next;

   protected Log log;
   protected boolean trace;

   protected Configuration configuration;

   public CommandInterceptor() {
      log = LogFactory.getLog(getClass());
      trace = log.isTraceEnabled();
   }

   @Inject
   private void injectConfiguration(Configuration configuration) {
      this.configuration = configuration;
   }

   /**
    * Retrieves the next interceptor in the chain.
    *
    * @return the next interceptor in the chain.
    */
   public final CommandInterceptor getNext() {
      return next;
   }

   /**
    * @return true if there is another interceptor in the chain after this; false otherwise.
    */
   public final boolean hasNext() {
      return getNext() != null;
   }

   /**
    * Sets the next interceptor in the chain to the interceptor passed in.
    *
    * @param next next interceptor in the chain.
    */
   public final void setNext(CommandInterceptor next) {
      this.next = next;
   }

   /**
    * Invokes the next interceptor in the chain.  This is how interceptor implementations should pass a call up the
    * chain to the next interceptor.
    *
    * @param ctx     invocation context
    * @param command command to pass up the chain.
    * @return return value of the invocation
    * @throws Throwable in the event of problems
    */
   public final Object invokeNextInterceptor(InvocationContext ctx, VisitableCommand command) throws Throwable {
      return command.acceptVisitor(ctx, next);
   }

   /**
    * The default behaviour of the visitXXX methods, which is to ignore the call and pass the call up to the next
    * interceptor in the chain.
    *
    * @param ctx     invocation context
    * @param command command to invoke
    * @return return value
    * @throws Throwable in the event of problems
    */
   @Override
   protected Object handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
      return invokeNextInterceptor(ctx, command);
   }
}