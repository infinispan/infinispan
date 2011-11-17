package org.infinispan.configuration.global;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.infinispan.marshall.AdvancedExternalizer;
import org.infinispan.marshall.Marshaller;

public class SerializationConfiguration {

   private final Class<? extends Marshaller> marshallerClass;
   private final short version;
   private final Map<Integer, AdvancedExternalizer<?>> advancedExternalizers;
   
   SerializationConfiguration(Class<? extends Marshaller> marshallerClass, short version,
         Map<Integer, AdvancedExternalizer<?>> advancedExternalizers) {
      this.marshallerClass = marshallerClass;
      this.version = version;
      this.advancedExternalizers = Collections.unmodifiableMap(new HashMap<Integer, AdvancedExternalizer<?>>(advancedExternalizers));
   }

   public Class<? extends Marshaller> getMarshallerClass() {
      return marshallerClass;
   }

   public short getVersion() {
      return version;
   }
   
   public Map<Integer, AdvancedExternalizer<?>> getAdvancedExternalizers() {
      return advancedExternalizers;
   }
   
}