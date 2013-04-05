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

package org.infinispan.context;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.util.Equivalence;

import javax.transaction.TransactionManager;


/**
 * Base class for InvocationContextContainer implementations.
 *
 * @author Mircea Markus
 * @since 5.1
 */
public abstract class AbstractInvocationContextContainer implements InvocationContextContainer {

   // See ISPN-1397.  There is no real need to store the InvocationContext in a thread local at all, since it is passed
   // as a parameter to any component that requires it - except for two components at the moment that require reading
   // the InvocationContext from a thread local.  These two are the ClusterCacheLoader and the JBossMarshaller.  The
   // former can be fixed once the CacheStore SPI is changed to accept an InvocationContext (see ISPN-1416) and the
   // latter can be fixed once the CacheManager architecture is changed to be associated with a ClassLoader per
   // CacheManager (see ISPN-1413), after which this thread local can be removed and the getInvocationContext() method
   // can also be removed.
   protected final ThreadLocal<InvocationContext> ctxHolder = new ThreadLocal<InvocationContext>();

   protected Configuration config;
   protected Equivalence keyEq;

   public void init(Configuration config) {
      this.config = config;
   }

   public void start() {
      keyEq = config.dataContainer().keyEquivalence();
   }

   @Override
   public InvocationContext createRemoteInvocationContextForCommand(
         VisitableCommand cacheCommand, Address origin) {
      if (cacheCommand instanceof FlagAffectedCommand) {
         InvocationContext context = createRemoteInvocationContext(origin);
         ctxHolder.set(context);
         return context;
      } else {
         return this.createRemoteInvocationContext(origin);
      }
   }

   @Override
   public InvocationContext getInvocationContext(boolean quiet) {
      InvocationContext ctx = ctxHolder.get();
      if (ctx == null && !quiet) throw new IllegalStateException("No InvocationContext associated with current thread!");
      return ctx;
   }

   @Override
   public void clearThreadLocal() {
      ctxHolder.remove();
   }
}
