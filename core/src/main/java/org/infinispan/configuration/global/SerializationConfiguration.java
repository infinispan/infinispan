package org.infinispan.configuration.global;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.infinispan.marshall.AdvancedExternalizer;
import org.infinispan.marshall.Marshaller;

public class SerializationConfiguration {

   private final Marshaller marshaller;
   private final short version;
   private final Map<Integer, AdvancedExternalizer<?>> advancedExternalizers;
   
   SerializationConfiguration(Marshaller marshaller, short version,
         Map<Integer, AdvancedExternalizer<?>> advancedExternalizers) {
      this.marshaller = marshaller;
      this.version = version;
      this.advancedExternalizers = Collections.unmodifiableMap(new HashMap<Integer, AdvancedExternalizer<?>>(advancedExternalizers));
   }

   public Marshaller marshaller() {
      return marshaller;
   }

   public short version() {
      return version;
   }
   
   public Map<Integer, AdvancedExternalizer<?>> advancedExternalizers() {
      return advancedExternalizers;
   }

   @Override
   public String toString() {
      return "SerializationConfiguration{" +
            "advancedExternalizers=" + advancedExternalizers +
            ", marshaller=" + marshaller +
            ", version=" + version +
            '}';
   }

}