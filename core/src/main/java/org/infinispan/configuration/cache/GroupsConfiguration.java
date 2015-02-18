package org.infinispan.configuration.cache;

import java.util.LinkedList;
import java.util.List;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeInitializer;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.distribution.group.Group;
import org.infinispan.distribution.group.Grouper;

/**
 * Configuration for various grouper definitions. See the user guide for more information.
 *
 * @author pmuir
 *
 */
public class GroupsConfiguration {
   final static AttributeDefinition<Boolean> ENABLED = AttributeDefinition.builder("enabled", false).immutable().build();
   final static AttributeDefinition<List<Grouper<?>>> GROUPERS = AttributeDefinition.builder("groupers", null, (Class<List<Grouper<?>>>)(Class<?>)List.class).initializer(new AttributeInitializer<List<Grouper<?>>>() {
      @Override
      public List<Grouper<?>> initialize() {
         return new LinkedList<Grouper<?>>();
      }
   }).immutable().build();
   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(GroupsConfiguration.class, ENABLED, GROUPERS);
   }

   private final Attribute<Boolean> enabled;
   private final Attribute<List<Grouper<?>>> groupers;
   private final AttributeSet attributes;

   GroupsConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
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
    * Get's the current groupers in use
    */
   public List<Grouper<?>> groupers() {
      return groupers.get();
   }

   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public String toString() {
      return "GroupsConfiguration [attributes=" + attributes + "]";
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      GroupsConfiguration other = (GroupsConfiguration) obj;
      if (attributes == null) {
         if (other.attributes != null)
            return false;
      } else if (!attributes.equals(other.attributes))
         return false;
      return true;
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((attributes == null) ? 0 : attributes.hashCode());
      return result;
   }

}
