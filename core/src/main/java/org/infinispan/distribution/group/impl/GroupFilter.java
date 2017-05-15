package org.infinispan.distribution.group.impl;

import org.infinispan.filter.KeyFilter;

/**
 * A key filter that accepts keys which belongs to the group.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class GroupFilter<K> implements KeyFilter<K> {

   private final Object groupName;
   private final GroupManager groupManager;

   public GroupFilter(Object groupName, GroupManager groupManager) {
      this.groupName = groupName;
      this.groupManager = groupManager;
   }

   @Override
   public boolean accept(K key) {
      Object keyGroup = groupManager.getGroup(key);
      return keyGroup != null && keyGroup.equals(groupName);
   }
}
