package org.infinispan.distribution.group.impl;

import java.util.function.Predicate;

/**
 * A key filter that accepts keys which belongs to the group.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class GroupFilter<K> implements Predicate<K> {

   private final Object groupName;
   private final GroupManager groupManager;

   public GroupFilter(Object groupName, GroupManager groupManager) {
      this.groupName = groupName;
      this.groupManager = groupManager;
   }

   @Override
   public boolean test(K key) {
      Object keyGroup = groupManager.getGroup(key);
      return keyGroup != null && keyGroup.equals(groupName);
   }
}
