package org.infinispan.distribution.group.impl;

import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.distribution.group.GroupManager;

/**
 * Key partitioner that uses {@link org.infinispan.distribution.group.Group} annotations to map
 * grouped keys to the same segment.
 *
 * @author Dan Berindei
 * @since 8.2
 */
public class GroupingPartitioner implements KeyPartitioner {
   private final KeyPartitioner partitioner;
   private final GroupManager groupManager;

   public GroupingPartitioner(KeyPartitioner partitioner, GroupManager groupManager) {
      this.partitioner = partitioner;
      this.groupManager = groupManager;
   }

   @Override
   public int getSegment(Object key) {
      String groupKey = groupManager.getGroup(key);
      return partitioner.getSegment(groupKey != null ? groupKey : key);
   }

   public KeyPartitioner unwrap() {
      return partitioner;
   }
}
