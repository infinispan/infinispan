package org.infinispan.commons.util;

import java.util.Collection;
import java.util.Set;
import java.util.function.Function;

/**
 * A set that maps another one to a new one of a possibly different type.  Note this set is read only
 * and doesn't accept write operations except iterator removal and clear as they don't pertain to the values.
 * <p>
 * This class currently only accepts a {@link Function} that also implements {@link InjectiveFunction} so that it can
 * guarantee the resulting mapped values are distinct from each other.  This is important as many operations because
 * very costly if this is not true.
 * <p>
 * Some operations such as {@link Collection#contains(Object)} and {@link Collection#containsAll(Collection)} may be
 * more expensive then normal since they cannot utilize lookups into the original collection.
 * @author wburns
 * @since 16.0
 */
public class SetMapper<E, R> extends CollectionMapper<E, R> implements Set<R> {
   public SetMapper(Set<E> realCollection, InjectiveFunction<? super E, ? extends R> mapper) {
      super(realCollection, mapper);
   }
}
