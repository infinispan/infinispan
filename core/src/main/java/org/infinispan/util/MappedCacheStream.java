package org.infinispan.util;

import java.util.Set;

import org.infinispan.CacheStream;
import org.infinispan.commons.util.InjectiveFunction;

/**
 * CacheStream that allows for mapping the filtered keys to different keys. Note that the function provided
 * must be a {@link InjectiveFunction} guaranteeing that keys are distinct when mapped.
 * @author wburns
 * @since 9.2
 */
class MappedCacheStream<R> extends AbstractDelegatingCacheStream<R> {
   private final InjectiveFunction<Object, ?> keyMapper;
   MappedCacheStream(CacheStream<R> stream, InjectiveFunction<Object, ?> keyMapper) {
      super(stream);
      this.keyMapper = keyMapper;
   }

   @Override
   public AbstractDelegatingCacheStream<R> filterKeys(Set<?> keys) {
      return super.filterKeys(new SetMapper<>(keys, keyMapper));
   }
}
