package org.infinispan.distribution.group;

import org.infinispan.filter.KeyFilter;

/**
 * A key filter that accepts keys which belongs to the group.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class GroupFilter<K> implements KeyFilter<K> {

   private final String groupName;
   private final GroupManager groupManager;

   public GroupFilter(String groupName, GroupManager groupManager) {
      this.groupName = groupName;
      this.groupManager = groupManager;
   }

   @Override
   public boolean accept(K key) {
      String keyGroup = groupManager.getGroup(key);
      return keyGroup != null && keyGroup.equals(groupName);
   }
}
