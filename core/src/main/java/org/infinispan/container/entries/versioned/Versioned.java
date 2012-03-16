/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

package org.infinispan.container.entries.versioned;

import org.infinispan.container.versioning.EntryVersion;

/**
 * An interface that marks the ability to handle versions
 *
 * @author Manik Surtani
 * @since 5.1
 */
public interface Versioned {

   /**
    * @return the version of the entry.  May be null if versioning is not supported, and must never be null if
    *         versioning is supported.
    */
   EntryVersion getVersion();

   /**
    * Sets the version on this entry.
    *
    * @param version version to set
    */
   void setVersion(EntryVersion version);
}
