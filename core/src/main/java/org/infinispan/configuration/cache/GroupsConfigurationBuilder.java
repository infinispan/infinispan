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

import org.infinispan.distribution.group.Group;
import org.infinispan.distribution.group.Grouper;

import java.util.LinkedList;
import java.util.List;

/**
 * Configuration for various grouper definitions. See the user guide for more information.
 * 
 * @author pmuir
 * 
 */
public class GroupsConfigurationBuilder extends AbstractClusteringConfigurationChildBuilder<GroupsConfiguration> {

   private boolean enabled = false;
   private List<Grouper<?>> groupers = new LinkedList<Grouper<?>>();

   protected GroupsConfigurationBuilder(ClusteringConfigurationBuilder builder) {
      super(builder);
   }
   
   /**
    * Enable grouping support so that {@link Group} annotations are honored and any configured
    * groupers will be invoked
    */
   public GroupsConfigurationBuilder enabled() {
      this.enabled = true;
      return this;
   }
   
   /**
    * Enable grouping support so that {@link Group} annotations are honored and any configured
    * groupers will be invoked
    */
   public GroupsConfigurationBuilder enabled(boolean enabled) {
      this.enabled = enabled;
      return this;
   }
   
   /**
    * Disable grouping support so that {@link Group} annotations are not used and any configured
    * groupers will not be be invoked
    */
   public GroupsConfigurationBuilder disabled() {
      this.enabled = false;
      return this;
   }

   /**
    * Set the groupers to use
    */
   public GroupsConfigurationBuilder withGroupers(List<Grouper<?>> groupers) {
      this.groupers = groupers;
      return this;
   }
   
   /**
    * Clear the groupers
    */
   public GroupsConfigurationBuilder clearGroupers() {
      this.groupers = new LinkedList<Grouper<?>>();
      return this;
   }
   
   /**
    * Add a grouper
    */
   public GroupsConfigurationBuilder addGrouper(Grouper<?> grouper) {
      this.groupers.add(grouper);
      return this;
   }

   @Override
   void validate() {
      // TODO Auto-generated method stub

   }

   @Override
   GroupsConfiguration create() {
      return new GroupsConfiguration(enabled, groupers);
   }

   @Override
   public GroupsConfigurationBuilder read(GroupsConfiguration template) {
      this.enabled = template.enabled();
      this.groupers = template.groupers();
      
      return this;
   }
   
}
