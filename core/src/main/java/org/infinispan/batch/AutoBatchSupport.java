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
package org.infinispan.batch;

import net.jcip.annotations.NotThreadSafe;
import org.infinispan.config.Configuration;
import org.infinispan.config.ConfigurationException;

/**
 * Enables for automatic batching.
 *
 * @author Manik Surtani (<a href="mailto:manik AT jboss DOT org">manik AT jboss DOT org</a>)
 * @since 4.0
 */
@NotThreadSafe
public abstract class AutoBatchSupport {
   protected BatchContainer batchContainer;

   protected static void assertBatchingSupported(Configuration c) {
      if (!c.isInvocationBatchingEnabled())
         throw new ConfigurationException("Invocation batching not enabled in current configuration! Please enable it.");
   }

   protected void startAtomic() {
      batchContainer.startBatch(true);
   }

   protected void endAtomic() {
      batchContainer.endBatch(true, true);
   }
}
