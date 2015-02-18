package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.GroupsConfiguration.ENABLED;
import static org.infinispan.configuration.cache.GroupsConfiguration.GROUPERS;

import java.util.List;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.distribution.group.Group;
import org.infinispan.distribution.group.Grouper;
/**
 * Configuration for various grouper definitions. See the user guide for more information.
 *
 * @author pmuir
 *
 */
public class GroupsConfigurationBuilder extends AbstractClusteringConfigurationChildBuilder implements Builder<GroupsConfiguration> {

   private final AttributeSet attributes;
   protected GroupsConfigurationBuilder(ClusteringConfigurationBuilder builder) {
      super(builder);
      attributes = GroupsConfiguration.attributeDefinitionSet();
   }

   /**
    * Enable grouping support so that {@link Group} annotations are honored and any configured
    * groupers will be invoked
    */
   public GroupsConfigurationBuilder enabled() {
      attributes.attribute(ENABLED).set(true);
      return this;
   }

   /**
    * Enable grouping support so that {@link Group} annotations are honored and any configured
    * groupers will be invoked
    */
   public GroupsConfigurationBuilder enabled(boolean enabled) {
      attributes.attribute(ENABLED).set(enabled);
      return this;
   }

   /**
    * Disable grouping support so that {@link Group} annotations are not used and any configured
    * groupers will not be be invoked
    */
   public GroupsConfigurationBuilder disabled() {
      attributes.attribute(ENABLED).set(false);
      return this;
   }

   /**
    * Set the groupers to use
    */
   public GroupsConfigurationBuilder withGroupers(List<Grouper<?>> groupers) {
      attributes.attribute(GROUPERS).set(groupers);
      return this;
   }

   /**
    * Clear the groupers
    */
   public GroupsConfigurationBuilder clearGroupers() {
      List<Grouper<?>> groupers = attributes.attribute(GROUPERS).get();
      groupers.clear();
      attributes.attribute(GROUPERS).set(groupers);
      return this;
   }

   /**
    * Add a grouper
    */
   public GroupsConfigurationBuilder addGrouper(Grouper<?> grouper) {
      List<Grouper<?>> groupers = attributes.attribute(GROUPERS).get();
      groupers.add(grouper);
      attributes.attribute(GROUPERS).set(groupers);
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
      return new GroupsConfiguration(attributes.protect());
   }

   @Override
   public GroupsConfigurationBuilder read(GroupsConfiguration template) {
      attributes.read(template.attributes());

      return this;
   }

   @Override
   public String toString() {
      return "GroupsConfigurationBuilder [attributes=" + attributes + "]";
   }
}
