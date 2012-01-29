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
package org.infinispan.configuration.cache;

import java.util.List;

import org.infinispan.distribution.group.Group;
import org.infinispan.distribution.group.Grouper;

/**
 * Configuration for various grouper definitions. See the user guide for more information.
 * 
 * @author pmuir
 * 
 */
public class GroupsConfiguration {

   private final boolean enabled;
   private final List<Grouper<?>> groupers;

   GroupsConfiguration(boolean enabled, List<Grouper<?>> groupers) {
      this.enabled = enabled;
      this.groupers = groupers;
   }

   /**
    * If grouping support is enabled, then {@link Group} annotations are honored and any configured
    * groupers will be invoked
    * 
    * @param enabled
    * @return
    */
   public boolean enabled() {
      return enabled;
   }

   /**
    * Get's the current groupers in use
    */
   public List<Grouper<?>> groupers() {
      return groupers;
   }

}
