package org.infinispan.cli.completers;

import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

import org.infinispan.cli.Context;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.0
 **/
public abstract class EnumCompleter<E extends Enum<E>> extends ListCompleter {

   private final Set<String> enumSet;

   public EnumCompleter(Class<E> theEnum) {
      EnumSet<E> all = EnumSet.allOf(theEnum);
      this.enumSet = new HashSet<>(all.size());
      all.forEach(e -> enumSet.add(e.name()));
   }

   @Override
   Collection<String> getAvailableItems(Context context) {
      return enumSet;
   }
}
