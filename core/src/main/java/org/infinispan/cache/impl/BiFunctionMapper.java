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
public class BiFunctionMapper implements BiFunction {

   private final DataConversion keyDataConversion;
   private final DataConversion valueDataConversion;

   private final BiFunction biFunction;

   @Inject
   public void injectDependencies(ComponentRegistry componentRegistry) {
      componentRegistry.wireDependencies(keyDataConversion);
      componentRegistry.wireDependencies(valueDataConversion);
   }

   public BiFunctionMapper(BiFunction remappingFunction,
                           DataConversion keyDataConversion,
                           DataConversion valueDataConversion) {
      this.biFunction = remappingFunction;
      this.keyDataConversion = keyDataConversion;
      this.valueDataConversion = valueDataConversion;
   }

   @ProtoFactory
   BiFunctionMapper(MarshallableObject<BiFunction> biFunction, DataConversion keyDataConversion,
                    DataConversion valueDataConversion) {
      this.biFunction = MarshallableObject.unwrap(biFunction);
      this.keyDataConversion = keyDataConversion;
      this.valueDataConversion = valueDataConversion;
   }

   @ProtoField(1)
   MarshallableObject<BiFunction> getBiFunction() {
      return MarshallableObject.create(biFunction);
   }

   @ProtoField(2)
   public DataConversion getKeyDataConversion() {
      return keyDataConversion;
   }

   @ProtoField(3)
   public DataConversion getValueDataConversion() {
      return valueDataConversion;
   }

   @Override
   public Object apply(Object k, Object v) {
      Object key = keyDataConversion.fromStorage(k);
      Object value = valueDataConversion.fromStorage(v);
      Object result = biFunction.apply(key, value);
      return result != null ? valueDataConversion.toStorage(result) : null;
   }
}
