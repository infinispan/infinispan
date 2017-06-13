package org.infinispan.commands.functional;

import org.infinispan.functional.EntryView;

/**
 * Simplified version of functional command used for read-only operations after transactional modifications.
 */
public interface Mutation<K, V, R> {

   /**
    * @return Internal identifier used for purposes of marshalling
    */
   byte type();

   /**
    * Mutate the view
    *
    * @param view
    */
   R apply(EntryView.ReadWriteEntryView<K, V> view);
}
