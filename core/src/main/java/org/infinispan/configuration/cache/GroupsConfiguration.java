package org.infinispan.configuration.cache;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.configuration.parsing.Element;
import org.infinispan.distribution.group.Group;
import org.infinispan.distribution.group.Grouper;

/**
 * Configuration for various grouper definitions. See the user guide for more information.
 *
 * @author pmuir
 *
 */
public class GroupsConfiguration extends ConfigurationElement<GroupsConfiguration> {
   public static final AttributeDefinition<Boolean> ENABLED = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.ENABLED, false).immutable().build();
   public static final AttributeDefinition<List<Grouper<?>>> GROUPERS = AttributeDefinition.builder(Element.GROUPER, null, (Class<List<Grouper<?>>>) (Class<?>) List.class).initializer(ArrayList::new)
         .immutable().build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(GroupsConfiguration.class, ENABLED, GROUPERS);
   }

   private final Attribute<Boolean> enabled;
   private final Attribute<List<Grouper<?>>> groupers;

   GroupsConfiguration(AttributeSet attributes) {
      super(Element.GROUPS, attributes);
      enabled = attributes.attribute(ENABLED);
      groupers = attributes.attribute(GROUPERS);
   }

   /**
    * If grouping support is enabled, then {@link Group} annotations are honored and any configured
    * groupers will be invoked
    *
    * @return
    */
   public boolean enabled() {
      return enabled.get();
   }

   /**
    * Get the current groupers in use
    */
   public List<Grouper<?>> groupers() {
      return groupers.get();
   }
}
