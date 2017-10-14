package org.infinispan.commands.functional;

import org.infinispan.encoding.DataConversion;
import org.infinispan.functional.impl.Params;

/**
 * A command that carries operation rather than final value.
 */
public interface FunctionalCommand<K, V> {

   Params getParams();
   Mutation<K, V, ?> toMutation(Object key);

   DataConversion getKeyDataConversion();

   DataConversion getValueDataConversion();
}
