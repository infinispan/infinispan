package org.infinispan.commons.util;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Consumer;

/**
 * An Iterator implementation that allows for a Iterator that doesn't allow remove operations to
 * implement remove by delegating the call to the provided consumer to remove the previously read value.
 *
 * @author wburns
 * @since 9.1
 */
public class RemovableIterator<C> implements Iterator<C> {
   protected final Iterator<C> realIterator;
   protected final Consumer<? super C> consumer;

   protected C previousValue;
   protected C currentValue;

   public RemovableIterator(Iterator<C> realIterator, Consumer<? super C> consumer) {
      this.realIterator = realIterator;
      this.consumer = consumer;
   }

   protected C getNextFromIterator() {
      if (realIterator.hasNext()) {
         return realIterator.next();
      } else {
         return null;
      }
   }

   @Override
   public boolean hasNext() {
      return currentValue != null || (currentValue = getNextFromIterator()) != null;
   }

   @Override
   public C next() {
      if (!hasNext()) {
         throw new NoSuchElementException();
      }
      previousValue = currentValue;
      currentValue = null;
      return previousValue;
   }

   @Override
   public void remove() {
      if (previousValue == null) {
         throw new IllegalStateException();
      }
      consumer.accept(previousValue);
      previousValue = null;
   }
}
