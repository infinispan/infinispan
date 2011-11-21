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
