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

import org.infinispan.container.DataContainer;

/**
 * A marker entry to represent a null for repeatable read, so that a read that returns a null can continue to return
 * null.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
public class NullMarkerEntry extends NullMarkerEntryForRemoval {

   private static final NullMarkerEntry INSTANCE = new NullMarkerEntry();

   private NullMarkerEntry() {
      super(null, null);
   }

   public static NullMarkerEntry getInstance() {
      return INSTANCE;
   }

   /**
    * A no-op.
    */
   @Override
   public final void copyForUpdate(DataContainer d, boolean localModeWriteSkewCheck) {
      // no op
   }
}