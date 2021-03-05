package org.infinispan.container.entries;

import static org.infinispan.container.entries.ReadCommittedEntry.Flags.SKIP_LOOKUP;

import org.infinispan.metadata.Metadata;

/**
 * An extension of {@link ReadCommittedEntry} that provides Repeatable Read semantics
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
public class RepeatableReadEntry<K, V> extends ReadCommittedEntry<K, V> {

   public RepeatableReadEntry(K key, V value, Metadata metadata) {
      super(key, value, metadata);
   }

   @Override
   public void setSkipLookup(boolean skipLookup) {
      setFlag(skipLookup, SKIP_LOOKUP);
   }

   @Override
   public boolean skipLookup() {
      return isFlagSet(SKIP_LOOKUP);
   }

   @Override
   public RepeatableReadEntry<K, V> clone() {
      return (RepeatableReadEntry<K, V>) super.clone();
   }

   @Override
   public final V setValue(V value) {
      V prev = super.setValue(value);
      setSkipLookup(true);
      return prev;
   }

   @Override
   public void setRead() {
      setFlag(Flags.READ);
   }

   @Override
   public boolean isRead() {
      return isFlagSet(Flags.READ);
   }
}
