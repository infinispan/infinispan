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
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.query.logging.Log;
import org.infinispan.util.logging.LogFactory;

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

   private static final Log log = LogFactory.getLog(LocalQueryInterceptor.class, Log.class);

   public LocalQueryInterceptor(SearchFactoryIntegrator searchFactory) {
      super(searchFactory);
   }

   @Override
   protected Log getLog() {
      return log;
   }

   @Override
   protected boolean shouldModifyIndexes(FlagAffectedCommand command, InvocationContext ctx) {
      // will index only local updates that were not flagged with SKIP_INDEXING and are not caused internally by state transfer
      return ctx.isOriginLocal() && !command.hasFlag(Flag.SKIP_INDEXING) && !command.hasFlag(Flag.PUT_FOR_STATE_TRANSFER);
   }
}
