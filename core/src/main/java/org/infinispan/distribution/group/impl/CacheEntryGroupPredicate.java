package org.infinispan.distribution.group.impl;

import java.util.Objects;
import java.util.function.Predicate;

import org.infinispan.commands.functional.functions.InjectableComponent;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Tests if a {@link CacheEntry} belongs to a group.
 *
 * @since 14.0
 */
@ProtoTypeId(ProtoStreamTypeIds.CACHE_ENTRY_GROUP_PREDICATE)
public class CacheEntryGroupPredicate<K> implements Predicate<CacheEntry<K, ?>>, InjectableComponent {

   private static final Log log = LogFactory.getLog(CacheEntryGroupPredicate.class);

   private GroupManager groupManager;

   @ProtoField(1)
   final String groupName;

   @ProtoFactory
   public CacheEntryGroupPredicate(String groupName) {
      this.groupName = groupName;
   }


   @Override
   public boolean test(CacheEntry<K, ?> entry) {
      String keyGroup = String.valueOf(groupManager.getGroup(entry.getKey()));
      boolean sameGroup = Objects.equals(groupName, keyGroup);
      if (log.isTraceEnabled()) {
         log.tracef("Testing key %s for group %s. Same group? %s", entry.getKey(), groupName, sameGroup);
      }
      return sameGroup;
   }

   @Override
   public void inject(ComponentRegistry registry) {
      groupManager = registry.getComponent(GroupManager.class);
   }

   @Override
   public String toString() {
      return "CacheEntryGroupPredicate{" +
            "groupName='" + groupName + '\'' +
            '}';
   }
}
