package org.infinispan.jcache.embedded.functions;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

import javax.cache.CacheException;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.processor.EntryProcessor;

import org.infinispan.commands.functional.functions.InjectableComponent;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.util.Util;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.functional.EntryView;
import org.infinispan.functional.MetaParam;
import org.infinispan.jcache.Exceptions;
import org.infinispan.jcache.Expiration;
import org.infinispan.jcache.embedded.Durations;
import org.infinispan.jcache.embedded.ExternalizerIds;

public class Invoke<K, V, R> implements Function<EntryView.ReadWriteEntryView<K, V>, R>, InjectableComponent {
   private final EntryProcessor<K, V, R> processor;
   private final Object[] arguments;
   private final boolean storeByReference;
   private StreamingMarshaller marshaller;
   private ExpiryPolicy expiryPolicy;

   public Invoke(EntryProcessor<K, V, R> processor, Object[] arguments, boolean storeByReference) {
      this.processor = processor;
      this.arguments = arguments;
      this.storeByReference = storeByReference;
   }

   @Override
   public void inject(ComponentRegistry registry) {
      this.marshaller = registry.getComponent(StreamingMarshaller.class);
      expiryPolicy = registry.getComponent(ExpiryPolicy.class);
   }

   @Override
   public R apply(EntryView.ReadWriteEntryView<K, V> view) {
      V oldValue = view.find().orElse(null);
      Supplier<V> supplier;
      if (oldValue == null) {
         supplier = () -> null;
      } else {
         supplier = () -> copy(oldValue);
      }
      MutableEntryWrapper<K, V> entry = new MutableEntryWrapper<>(view, supplier, oldValue != null, expiryPolicy);
      R retval;
      try {
         retval = processor.process(entry, arguments);
      } catch (Exception e) {
         throw Exceptions.launderEntryProcessorException(e);
      }
      // If the MutableEntry is wrapped in some other object we won't marshall it. We could proxy the object
      // during marshalling but that is executed without the lock and wouldn't set access ttl.
      boolean readByReturn = false;
      if (retval instanceof MutableEntryWrapper) {
         readByReturn = true;
         retval = (R) new MutableEntrySnapshot<>(view.key(), view.find().orElse(null));
      }

      if (!entry.isModified() && (entry.isRead() || readByReturn)) {
         boolean loaded = view.findMetaParam(MetaParam.MetaLoadedFromPersistence.class).map(MetaParam::get).orElse(false);
         Durations.updateTtl(view, expiryPolicy, loaded ? Expiration.Operation.CREATION : Expiration.Operation.ACCESS);
      }
      return retval;
   }

   private V copy(V original) {
      // we're not allowed to create a copy when storing by reference, despite this could
      // cause inconsistencies, applying the operation many times etc.
      if (storeByReference) {
         return original;
      }
      try {
         byte[] bytes = marshaller.objectToByteBuffer(original);
         Object o = marshaller.objectFromByteBuffer(bytes);
         return (V) o;
      } catch (Exception e) {
         throw new CacheException(
               "Unexpected error making a copy of entry " + original, e);
      }
   }

   public static class Externalizer implements AdvancedExternalizer<Invoke> {
      @Override
      public Set<Class<? extends Invoke>> getTypeClasses() {
         return Util.asSet(Invoke.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.INVOKE;
      }

      @Override
      public void writeObject(ObjectOutput output, Invoke object) throws IOException {
         output.writeObject(object.processor);
         MarshallUtil.marshallArray(object.arguments, output);
         output.writeBoolean(object.storeByReference);
      }

      @Override
      public Invoke readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new Invoke((EntryProcessor) input.readObject(),
               MarshallUtil.unmarshallArray(input, Util::objectArray), input.readBoolean());
      }
   }
}
