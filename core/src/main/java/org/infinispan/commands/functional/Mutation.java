package org.infinispan.commands.functional;

import org.infinispan.commands.functional.functions.InjectableComponent;
import org.infinispan.encoding.DataConversion;
import org.infinispan.functional.EntryView;

/**
 * Simplified version of functional command used for read-only operations after transactional modifications.
 */
public interface Mutation<K, V, R> extends InjectableComponent {

   /**
    * @return Internal identifier used for purposes of marshalling
    */
   byte type();

   /**
    * Mutate the view
    *  @param view
    * */
   R apply(EntryView.ReadWriteEntryView<K, V> view);

   DataConversion keyDataConversion();

   DataConversion valueDataConversion();
}
