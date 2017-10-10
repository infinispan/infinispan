package org.infinispan.lock.impl.lock;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.filter.KeyFilter;
import org.infinispan.lock.impl.entries.ClusteredLockKey;
import org.infinispan.lock.impl.externalizers.ExternalizerIds;

/**
 * This listener is used to monitor lock state changes.
 * More about listeners {@see http://infinispan.org/docs/stable/user_guide/user_guide.html#cache_level_notifications}
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
public class ClusteredLockFilter implements KeyFilter<ClusteredLockKey> {

   public static final AdvancedExternalizer<ClusteredLockFilter> EXTERNALIZER = new ClusteredLockFilter.Externalizer();

   private final ClusteredLockKey name;

   public ClusteredLockFilter(ClusteredLockKey name) {
      this.name = name;
   }

   @Override
   public boolean accept(ClusteredLockKey key) {
      return name.equals(key);
   }

   public static class Externalizer extends AbstractExternalizer<ClusteredLockFilter> {
      @Override
      public Set<Class<? extends ClusteredLockFilter>> getTypeClasses() {
         return Collections.singleton(ClusteredLockFilter.class);
      }

      @Override
      public void writeObject(ObjectOutput output, ClusteredLockFilter object) throws IOException {
         output.writeObject(object.name);
      }

      @Override
      public ClusteredLockFilter readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new ClusteredLockFilter((ClusteredLockKey) input.readObject());
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.CLUSTERED_LOCK_FILTER;
      }
   }
}
