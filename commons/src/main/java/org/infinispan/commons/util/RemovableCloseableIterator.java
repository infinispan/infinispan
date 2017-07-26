package org.infinispan.commons.util;

import java.util.function.Consumer;

/**
 * A CloseableIterator implementation that allows for a CloseableIterator that doesn't allow remove operations to
 * implement remove by delegating the call to the provided consumer to remove the previously read value.
 *
 * @author wburns
 * @since 9.1
 */
public class RemovableCloseableIterator<C> extends RemovableIterator<C> implements CloseableIterator<C> {
   protected final CloseableIterator<C> realIterator;

   public RemovableCloseableIterator(CloseableIterator<C> realIterator, Consumer<? super C> consumer) {
      super(realIterator, consumer);
      this.realIterator = realIterator;
   }

   @Override
   public void close() {
      currentValue = null;
      previousValue = null;
      realIterator.close();
   }
}
