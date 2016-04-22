package org.infinispan.commons.util;

import java.util.Set;

/**
 * Adapts {@link Set} to {@link CloseableIteratorSet}
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class CloseableIteratorSetAdapter<E> extends CloseableIteratorCollectionAdapter<E> implements CloseableIteratorSet<E> {
   public CloseableIteratorSetAdapter(Set<E> delegate) {
      super(delegate);
   }

   @Override
   public CloseableSpliterator<E> spliterator() {
      return Closeables.spliterator(delegate.spliterator());
   }
}
