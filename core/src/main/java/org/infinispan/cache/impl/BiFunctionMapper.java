package org.infinispan.cache.impl;

import java.util.function.BiFunction;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * A Bifuncion wrapper that uses the cache's underlying DataConversion objects to perform its operations.
 */
@ProtoTypeId(ProtoStreamTypeIds.BI_FUNCTION_MAPPER)
@Scope(Scopes.NAMED_CACHE)
public class BiFunctionMapper<T, U, R> implements BiFunction<T, U, R> {

   private final DataConversion keyDataConversion;
   private final DataConversion valueDataConversion;
   private final BiFunction<T, U, R> biFunction;

   @Inject
   public void injectDependencies(ComponentRegistry componentRegistry) {
      componentRegistry.wireDependencies(keyDataConversion);
      componentRegistry.wireDependencies(valueDataConversion);
   }

   public BiFunctionMapper(BiFunction<T, U, R> remappingFunction,
                           DataConversion keyDataConversion,
                           DataConversion valueDataConversion) {
      this.biFunction = remappingFunction;
      this.keyDataConversion = keyDataConversion;
      this.valueDataConversion = valueDataConversion;
   }

   @ProtoFactory
   BiFunctionMapper(MarshallableObject<BiFunction<T, U, R>> biFunction, DataConversion keyDataConversion,
                    DataConversion valueDataConversion) {
      this.biFunction = MarshallableObject.unwrap(biFunction);
      this.keyDataConversion = keyDataConversion;
      this.valueDataConversion = valueDataConversion;
   }

   @ProtoField(number = 1)
   MarshallableObject<BiFunction<T, U, R>> getBiFunction() {
      return MarshallableObject.create(biFunction);
   }

   @ProtoField(number = 2)
   public DataConversion getKeyDataConversion() {
      return keyDataConversion;
   }

   @ProtoField(number = 3)
   public DataConversion getValueDataConversion() {
      return valueDataConversion;
   }

   @Override
   @SuppressWarnings("unchecked")
   public R apply(T k, U v) {
      T key = (T) keyDataConversion.fromStorage(k);
      U value = (U) valueDataConversion.fromStorage(v);
      R result = biFunction.apply(key, value);
      return result != null ? (R) valueDataConversion.toStorage(result) : null;
   }
}
