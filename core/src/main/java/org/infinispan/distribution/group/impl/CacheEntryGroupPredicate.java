package org.infinispan.distribution.group.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

import org.infinispan.commands.functional.functions.InjectableComponent;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Tests if a {@link CacheEntry} belongs to a group.
 *
 * @since 14.0
 */
public class CacheEntryGroupPredicate<K> implements Predicate<CacheEntry<K, ?>>, InjectableComponent {

   private static final Log log = LogFactory.getLog(CacheEntryGroupPredicate.class);

   @SuppressWarnings("rawtypes")
   public static final AbstractExternalizer<CacheEntryGroupPredicate> EXTERNALIZER = new Externalizer();

   private GroupManager groupManager;
   private final String groupName;

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

   @SuppressWarnings("rawtypes")
   private static final class Externalizer extends AbstractExternalizer<CacheEntryGroupPredicate> {

      @Override
      public Set<Class<? extends CacheEntryGroupPredicate>> getTypeClasses() {
         return Collections.singleton(CacheEntryGroupPredicate.class);
      }

      @Override
      public Integer getId() {
         return Ids.CACHE_ENTRY_GROUP_PREDICATE;
      }

      @Override
      public void writeObject(ObjectOutput output, CacheEntryGroupPredicate object) throws IOException {
         output.writeUTF(object.groupName);
      }

      @Override
      public CacheEntryGroupPredicate readObject(ObjectInput input) throws IOException {
         return new CacheEntryGroupPredicate(input.readUTF());
      }
   }
}
