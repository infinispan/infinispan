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

   @Override
   public String toString() {
      return "GroupsConfiguration{" +
            "enabled=" + enabled +
            ", groupers=" + groupers +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      GroupsConfiguration that = (GroupsConfiguration) o;

      if (enabled != that.enabled) return false;
      if (groupers != null ? !groupers.equals(that.groupers) : that.groupers != null)
         return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = (enabled ? 1 : 0);
      result = 31 * result + (groupers != null ? groupers.hashCode() : 0);
      return result;
   }

}
