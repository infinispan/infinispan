package org.infinispan.util;

import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.CloseableIteratorCollection;

import java.util.Collection;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

public abstract class AbstractDelegatingCloseableIteratorCollection<E> extends AbstractDelegatingCollection<E> {

   protected abstract CloseableIteratorCollection<E> delegate();

   @Override
   public CloseableIterator<E> iterator() {
      return delegate().iterator();
   }
}
