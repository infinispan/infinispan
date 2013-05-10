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

import org.infinispan.Metadata;
import org.infinispan.container.DataContainer;
import org.infinispan.transaction.WriteSkewException;
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

   public RepeatableReadEntry(Object key, Object value, Metadata metadata) {
      super(key, value, metadata);
   }

   @Override
   public void copyForUpdate(DataContainer container, boolean localModeWriteSkewCheck) {
      if (isChanged()) return; // already copied

      // mark entry as changed.
      setChanged(true);

      if (localModeWriteSkewCheck) {
         performLocalWriteSkewCheck(container, false);
      }
      // make a backup copy
      oldValue = value;
   }

   public void performLocalWriteSkewCheck(DataContainer container, boolean alreadyCopied) {
      // check for write skew.
      InternalCacheEntry ice = container.get(key);

      Object actualValue = ice == null ? null : ice.getValue();
      Object valueToCompare = alreadyCopied ? oldValue : value;
      // Note that this identity-check is intentional.  We don't *want* to call actualValue.equals() since that defeats the purpose.
      // the implicit "versioning" we have in R_R creates a new wrapper "value" instance for every update.
      if (actualValue != null && actualValue != valueToCompare) {
         log.unableToCopyEntryForUpdate(getKey());
         throw new WriteSkewException("Detected write skew.");
      }

      if (ice == null && !isCreated()) {
         // We still have a write-skew here.  When this wrapper was created there was an entry in the data container
         // (hence isCreated() == false) but 'ice' is now null.
         log.unableToCopyEntryForUpdate(getKey());
         throw new WriteSkewException("Detected write skew - concurrent removal of entry!");
      }
   }
}