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
package org.infinispan.container.entries;

import org.infinispan.CacheException;
import org.infinispan.container.DataContainer;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * An extension of {@link org.infinispan.container.ReadCommittedEntry} that provides Repeatable Read semantics
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
         Object actualValue = container.get(key);

         if (actualValue != null && actualValue != value) {
            String errormsg = new StringBuilder().append("Detected write skew on key [").append(getKey()).append("].  Another process has changed the entry since we last read it!").toString();
            if (log.isWarnEnabled()) log.warn(errormsg + ".  Unable to copy entry for update.");
            throw new CacheException(errormsg);
         }
      }

      // make a backup copy
      oldValue = value;
   }
}