package org.infinispan.util;

import java.util.Collection;

import org.infinispan.CacheSet;
import org.infinispan.commons.util.InjectiveFunction;

/**
 * A writeable cache set mapper that also has constant time operations for things such as
 * {@link Collection#contains(Object)} if the underlying Set does. Also implements the Set interface.
 * <p>
 * This set should be used for cases when a simple transformation of an element to another is all that is
 * needed by the underlying set.
 * <p>
 * This implementation is basically identical to {@link WriteableCacheCollectionMapper} except that this class
 * also implements {@link java.util.Set}.
 * @author wburns
 * @since 9.2
 */
public class WriteableCacheSetMapper<E, R> extends WriteableCacheCollectionMapper<E, R> implements CacheSet<R> {
   public WriteableCacheSetMapper(CacheSet<E> realSet,
         InjectiveFunction<? super E, ? extends R> toNewTypeFunction,
         InjectiveFunction<? super R, ? extends E> fromNewTypeFunction,
         InjectiveFunction<Object, ?> keyFilterFunction) {
      super(realSet, toNewTypeFunction, fromNewTypeFunction, keyFilterFunction);
   }

   public WriteableCacheSetMapper(CacheSet<E> realSet,
         InjectiveFunction<? super E, ? extends R> toNewTypeFunction,
         InjectiveFunction<? super E, ? extends R> toNewTypeIteratorFunction,
         InjectiveFunction<? super R, ? extends E> fromNewTypeFunction,
         InjectiveFunction<Object, ?> keyFilterFunction) {
      super(realSet, toNewTypeFunction, toNewTypeIteratorFunction, fromNewTypeFunction, keyFilterFunction);
   }
}
