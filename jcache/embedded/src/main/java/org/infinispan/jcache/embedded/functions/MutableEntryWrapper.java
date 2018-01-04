package org.infinispan.jcache.embedded.functions;

import static org.infinispan.jcache.Expiration.Operation.CREATION;
import static org.infinispan.jcache.Expiration.Operation.UPDATE;

import java.util.Objects;
import java.util.function.Supplier;

import javax.cache.expiry.ExpiryPolicy;
import javax.cache.processor.MutableEntry;

import org.infinispan.commons.util.ReflectionUtil;
import org.infinispan.functional.EntryView;
import org.infinispan.jcache.embedded.Durations;

class MutableEntryWrapper<K, V> implements MutableEntry<K, V> {
   private final EntryView.ReadWriteEntryView<K, V> view;
   private final Supplier<V> copySupplier;
   private final boolean existsBefore;
   private final ExpiryPolicy expiryPolicy;
   private V valueCopy;
   private boolean isRead;
   private boolean isModified;

   MutableEntryWrapper(EntryView.ReadWriteEntryView<K, V> view, Supplier<V> copySupplier, boolean existsBefore, ExpiryPolicy expiryPolicy) {
      this.view = view;
      this.copySupplier = copySupplier;
      this.existsBefore = existsBefore;
      this.expiryPolicy = expiryPolicy;
   }

   @Override
   public boolean exists() {
      if (!isRead && !isModified) {
         isRead = true;
         valueCopy = copySupplier.get();
      }
      return valueCopy != null;
   }

   @Override
   public void remove() {
      // JCache TCK requires that if an entry was read -> null, created and then updated
      // in single invocation the persistence layer is not written to.
      // However if the entry was not read, the persistence layer is written to.
      if (isRead) {
         view.remove();
      } else {
         view.set(null); // Force the delete to be propagated to persistence
      }
      valueCopy = null;
      isRead = true;
      isModified = true;
   }

   @Override
   public K getKey() {
      return view.key();
   }

   @Override
   public V getValue() {
      if (!isRead && !isModified) {
         isRead = true;
         valueCopy = copySupplier.get();
      }
      return valueCopy;
   }

   @Override
   public <T> T unwrap(Class<T> clazz) {
      return ReflectionUtil.unwrap(this, clazz);
   }

   @Override
   public void setValue(V value) {
      Objects.requireNonNull(value, "Value must not be null");
      isModified = true;
      valueCopy = value;
      Durations.setWithTtl(view, value, expiryPolicy, existsBefore ? UPDATE : CREATION);
   }

   boolean isRead() {
      return isRead;
   }

   boolean isModified() {
      return isModified;
   }
}
