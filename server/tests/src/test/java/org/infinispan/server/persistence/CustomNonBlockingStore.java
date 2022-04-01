package org.infinispan.server.persistence;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;

import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.AdvancedCache;
import org.infinispan.encoding.DataConversion;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.MarshallableEntryFactory;
import org.infinispan.persistence.spi.NonBlockingStore;
import org.infinispan.commons.util.concurrent.CompletableFutures;

public class CustomNonBlockingStore implements NonBlockingStore<String, String> {
   private MarshallableEntryFactory<String, String> marshallableEntryFactory;
   private DataConversion keyDataConversion;
   private DataConversion valueDataConversion;

   @Override
   public CompletionStage<Void> start(InitializationContext ctx) {
      marshallableEntryFactory = ctx.getMarshallableEntryFactory();
      AdvancedCache<String, String> object = ctx.getCache().getAdvancedCache().withMediaType(APPLICATION_OBJECT, APPLICATION_OBJECT);
      keyDataConversion = object.getKeyDataConversion();
      valueDataConversion = object.getValueDataConversion();
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletionStage<Void> stop() {
      return CompletableFutures.completedNull();
   }

   @Override
   public Set<Characteristic> characteristics() {
      return EnumSet.of(Characteristic.READ_ONLY);
   }

   @Override
   public CompletionStage<MarshallableEntry<String, String>> load(int segment, Object storageKey) {
      Object objectKey = keyDataConversion.fromStorage(storageKey);
      String value = "Hello " + objectKey;
      Object storageValue = valueDataConversion.toStorage(value);
      return CompletableFuture.completedFuture(marshallableEntryFactory.create(storageKey, storageValue));
   }

   @Override
   public CompletionStage<Void> write(int segment, MarshallableEntry<? extends String, ? extends String> entry) {
      throw new UnsupportedOperationException("This store doesn't support write!");
   }

   @Override
   public CompletionStage<Boolean> delete(int segment, Object key) {
      throw new UnsupportedOperationException("This store doesn't support delete!");
   }

   @Override
   public CompletionStage<Void> clear() {
      throw new UnsupportedOperationException("This store doesn't support clear!");
   }
}
