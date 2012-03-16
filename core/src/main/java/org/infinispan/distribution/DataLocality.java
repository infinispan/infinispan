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
package org.infinispan.distribution;

/**
 * Used to determine whether a key is mapped to a local node.  Uncertainty indicates a rehash is in progress and the
 * locality of key in question may be in flux.
 *
 * @author Manik Surtani
 * @author Mircea Markus
 * @since 4.2.1
 */
public enum DataLocality {
   LOCAL(true,false),

   NOT_LOCAL(false,false),

   LOCAL_UNCERTAIN(true,true),

   NOT_LOCAL_UNCERTAIN(false,true);

   private final boolean local, uncertain;

   private DataLocality(boolean local, boolean uncertain) {
      this.local = local;
      this.uncertain = uncertain;
   }

   public boolean isLocal() {
      return local;
   }

   public boolean isUncertain() {
      return uncertain;
   }
}
