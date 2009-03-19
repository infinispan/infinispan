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
package org.horizon.invocation;

import org.horizon.context.InvocationContext;
import org.horizon.factories.annotations.Inject;
import org.horizon.factories.annotations.NonVolatile;
import org.horizon.factories.context.ContextFactory;
import org.horizon.factories.scopes.Scope;
import org.horizon.factories.scopes.Scopes;


/**
 * Container and factory for thread locals
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
@NonVolatile
@Scope(Scopes.NAMED_CACHE)
public class InvocationContextContainer extends ThreadLocal<InvocationContext> {
   ContextFactory contextFactory;

   @Inject
   public void injectContextFactory(ContextFactory contextFactory) {
      this.contextFactory = contextFactory;
   }

   @Override
   protected final InvocationContext initialValue() {
      // create if this is initially unset
      return contextFactory.createInvocationContext();
   }

   public void reset() {
      set(initialValue());
   }
}