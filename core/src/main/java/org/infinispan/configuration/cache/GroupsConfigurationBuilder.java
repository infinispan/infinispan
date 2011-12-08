package org.infinispan.configuration.cache;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.infinispan.distribution.group.Group;
import org.infinispan.distribution.group.Grouper;

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
      this.groupers = new ArrayList<Grouper<?>>();
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
