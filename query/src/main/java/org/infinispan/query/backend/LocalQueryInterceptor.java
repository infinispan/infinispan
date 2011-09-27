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
package org.infinispan.query.backend;

import org.hibernate.search.spi.SearchFactoryIntegrator;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;

import javax.transaction.TransactionManager;
import javax.transaction.TransactionSynchronizationRegistry;
import java.util.concurrent.ExecutorService;

/**
 * <p/>
 * This class is an interceptor that will index data only if it has come from a local source.
 * <p/>
 * Currently, this is a property that is determined by setting "infinispan.query.indexLocalOnly" as a System property to
 * "true".
 *
 * @author Navin Surtani
 * @since 4.0
 */
public class LocalQueryInterceptor extends QueryInterceptor {

   public LocalQueryInterceptor(SearchFactoryIntegrator searchFactory) {
      super(searchFactory);
   }

   // The Async Executor is injected here as well due to a limitation in the way core injects dependencies in
   // components that do not reside in core.  Essentially superclasses of components will *not* get scanned for
   // annotations if the superclass is itself not in core.
   @Inject
   public void injectDependencies(TransactionManager transactionManager,
                                  TransactionSynchronizationRegistry transactionSynchronizationRegistry,
                                  @ComponentName(KnownComponentNames.ASYNC_TRANSPORT_EXECUTOR) ExecutorService e) {
      // Fields on superclass.
      this.transactionManager = transactionManager;
      this.transactionSynchronizationRegistry = transactionSynchronizationRegistry;
      this.asyncExecutor = e;
   }

   @Override
   protected boolean shouldModifyIndexes(InvocationContext ctx) {
      return ctx.isOriginLocal() && ! ctx.hasFlag(Flag.SKIP_INDEXING);
   }
}
