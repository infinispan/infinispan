package org.infinispan.cache.impl;

import java.util.function.Function;

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

@ProtoTypeId(ProtoStreamTypeIds.FUNCTION_MAPPER)
@Scope(Scopes.NONE)
public class FunctionMapper implements Function {

   @ProtoField(1)
   final MarshallableObject<Function> function;

   @ProtoField(2)
   final DataConversion keyDataConversion;

   @ProtoField(3)
   final DataConversion valueDataConversion;

   @ProtoFactory
   FunctionMapper(MarshallableObject<Function> function, DataConversion keyDataConversion, DataConversion valueDataConversion) {
      this.function = function;
      this.keyDataConversion = keyDataConversion;
      this.valueDataConversion = valueDataConversion;
   }

   public FunctionMapper(Function mappingFunction,
                         DataConversion keyDataConversion,
                         DataConversion valueDataConversion) {
      this(MarshallableObject.create(mappingFunction), keyDataConversion, valueDataConversion);
   }

   @Inject
   public void injectDependencies(ComponentRegistry registry) {
      registry.wireDependencies(keyDataConversion);
      registry.wireDependencies(valueDataConversion);
   }

   @Override
   public Object apply(Object k) {
      Object key = keyDataConversion.fromStorage(k);
      Object result = function.get().apply(key);
      return result != null ? valueDataConversion.toStorage(result) : null;
   }
}
