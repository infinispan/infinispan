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
package org.infinispan.container.entries;

import org.infinispan.api.CacheException;
import org.infinispan.container.DataContainer;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * An extension of {@link ReadCommittedEntry} that provides Repeatable Read semantics
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
public class RepeatableReadEntry extends ReadCommittedEntry {
   private static final Log log = LogFactory.getLog(RepeatableReadEntry.class);

   public RepeatableReadEntry(Object key, Object value, long lifespan) {
      super(key, value, lifespan);
   }

   @Override
   public void copyForUpdate(DataContainer container, boolean writeSkewCheck) {
      if (isChanged()) return; // already copied

      // mark entry as changed.
      setChanged();

      if (writeSkewCheck) {
      // check for write skew.
         InternalCacheEntry ice = container.get(key);
         Object actualValue = ice == null ? null : ice.getValue();

         // Note that this identity-check is intentional.  We don't *want* to call actualValue.equals() since that defeats the purpose.
         // the implicit "versioning" we have in R_R creates a new wrapper "value" instance for every update.
         if (actualValue != null && actualValue != value) {
            log.unableToCopyEntryForUpdate(getKey());
            throw new CacheException("Detected write skew");
         }
      }
      // make a backup copy
      oldValue = value;
   }
}