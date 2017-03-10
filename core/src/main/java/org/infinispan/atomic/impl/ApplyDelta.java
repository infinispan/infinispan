package org.infinispan.atomic.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;
import java.util.function.BiFunction;

import org.infinispan.atomic.CopyableDeltaAware;
import org.infinispan.atomic.Delta;
import org.infinispan.atomic.DeltaAware;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.functional.EntryView;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.Util;
import org.infinispan.factories.GlobalComponentRegistry;

/**
 * Replacement for {@link org.infinispan.commands.write.ApplyDeltaCommand} and
 * {@link org.infinispan.context.Flag#DELTA_WRITE}. Deprecated since {@link Delta},
 * {@link org.infinispan.atomic.DeltaAware} and {@link org.infinispan.atomic.CopyableDeltaAware} are deprecated, too.
 */
// TODO: to be removed in Infinispan 10.0
public final class ApplyDelta<K> implements BiFunction<Object, EntryView.ReadWriteEntryView<K, Object>, Object> {
   private final Marshaller marshaller;

   public ApplyDelta(Marshaller marshaller) {
      this.marshaller = marshaller;
   }

   @Override
   public Object apply(Object d, EntryView.ReadWriteEntryView<K, Object> view) {
      if (!(d instanceof Delta)) {
         throw new IllegalArgumentException("Expected delta, argument is " + d);
      }
      Delta delta = (Delta) d;
      if (view.find().isPresent()) {
         Object value = view.find().get();
         DeltaAware deltaAware;
         if (value instanceof CopyableDeltaAware) {
            deltaAware = ((CopyableDeltaAware) value).copy();
         } else if (value instanceof DeltaAware) {
            try {
               byte[] bytes = marshaller.objectToByteBuffer(value);
               deltaAware = (DeltaAware) marshaller.objectFromByteBuffer(bytes);
            } catch (InterruptedException e) {
               Thread.currentThread().interrupt();
               throw new CacheException("Object copy interrupted", e);
            } catch (IOException | ClassNotFoundException e) {
               throw new CacheException("Cannot copy " + value, e);
            }
         } else {
            throw new IllegalArgumentException("Cache contains " + value + " which does not implement DeltaAware");
         }
         view.set(delta.merge(deltaAware));
         return value;
      } else {
         view.set(delta.merge(null));
         return null;
      }
   }

   @Deprecated
   public static class Externalizer implements AdvancedExternalizer<ApplyDelta> {
      private final GlobalComponentRegistry gcr;

      public Externalizer(GlobalComponentRegistry gcr) {
         this.gcr = gcr;
      }

      @Override
      public Set<Class<? extends ApplyDelta>> getTypeClasses() {
         return Util.asSet(ApplyDelta.class);
      }

      @Override
      public Integer getId() {
         return Ids.APPLY_DELTA;
      }

      @Override
      public void writeObject(ObjectOutput output, ApplyDelta object) throws IOException {
      }

      @Override
      public ApplyDelta readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new ApplyDelta(gcr.getComponent(StreamingMarshaller.class));
      }
   }
}
