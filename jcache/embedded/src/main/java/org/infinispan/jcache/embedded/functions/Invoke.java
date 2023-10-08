package org.infinispan.jcache.embedded.functions;

import java.util.function.Function;
import java.util.function.Supplier;

import javax.cache.CacheException;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.processor.EntryProcessor;

import org.infinispan.AdvancedCache;
import org.infinispan.commands.functional.functions.InjectableComponent;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.functional.EntryView;
import org.infinispan.functional.MetaParam;
import org.infinispan.jcache.Exceptions;
import org.infinispan.jcache.Expiration;
import org.infinispan.jcache.embedded.Durations;
import org.infinispan.marshall.protostream.impl.MarshallableArray;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

@ProtoTypeId(ProtoStreamTypeIds.JCACHE_INVOKE)
public class Invoke<K, V, R> implements Function<EntryView.ReadWriteEntryView<K, V>, R>, InjectableComponent {
   private final EntryProcessor<K, V, R> processor;
   private final Object[] arguments;
   private final boolean storeByReference;
   private ExpiryPolicy expiryPolicy;
   private DataConversion valueDataConversion;

   public Invoke(EntryProcessor<K, V, R> processor, Object[] arguments, boolean storeByReference) {
      this.processor = processor;
      this.arguments = arguments;
      this.storeByReference = storeByReference;
   }

   @ProtoFactory
   Invoke(MarshallableObject<EntryProcessor<K, V, R>> processor, MarshallableArray<Object> arguments, boolean storeByReference) {
      this.processor = MarshallableObject.unwrap(processor);
      this.arguments = MarshallableArray.unwrap(arguments);
      this.storeByReference = storeByReference;
   }

   @ProtoField(1)
   MarshallableObject<EntryProcessor<K, V, R>> getProcessor() {
      return MarshallableObject.create(processor);
   }

   @ProtoField(2)
   MarshallableArray<Object> getArguments() {
      return MarshallableArray.create(arguments);
   }

   @ProtoField(3)
   boolean isStoreByReference() {
      return storeByReference;
   }

   @Override
   public void inject(ComponentRegistry registry) {
      AdvancedCache<?, ?> advancedCache = registry.getCache().wired().getAdvancedCache();
      this.valueDataConversion = advancedCache.getValueDataConversion();
      this.expiryPolicy = registry.getComponent(ExpiryPolicy.class);
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
         Object asStored =  valueDataConversion.toStorage(original);
         Object o = valueDataConversion.fromStorage(asStored);
         return (V) o;
      } catch (Exception e) {
         throw new CacheException(
               "Unexpected error making a copy of entry " + original, e);
      }
   }
}
