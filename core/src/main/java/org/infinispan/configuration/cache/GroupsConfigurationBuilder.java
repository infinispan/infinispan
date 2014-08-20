package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.configuration.global.GlobalConfiguration;
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
public class GroupsConfigurationBuilder extends AbstractClusteringConfigurationChildBuilder implements Builder<GroupsConfiguration> {

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
   public void validate() {
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
   }

   @Override
   public GroupsConfiguration create() {
      return new GroupsConfiguration(enabled, groupers);
   }

   @Override
   public GroupsConfigurationBuilder read(GroupsConfiguration template) {
      this.enabled = template.enabled();
      this.groupers = template.groupers();

      return this;
   }

   @Override
   public String toString() {
      return "GroupsConfigurationBuilder{" +
            "enabled=" + enabled +
            ", groupers=" + groupers +
            '}';
   }
}
