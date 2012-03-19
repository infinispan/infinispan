package org.infinispan.configuration.global;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.infinispan.marshall.AdvancedExternalizer;
import org.infinispan.marshall.Marshaller;
import org.jboss.marshalling.ClassResolver;

public class SerializationConfiguration {

   private final Marshaller marshaller;
   private final short version;
   private final Map<Integer, AdvancedExternalizer<?>> advancedExternalizers;
   private final ClassResolver classResolver;
   
   SerializationConfiguration(Marshaller marshaller, short version,
         Map<Integer, AdvancedExternalizer<?>> advancedExternalizers,
         ClassResolver classResolver) {
      this.marshaller = marshaller;
      this.version = version;
      this.advancedExternalizers = Collections.unmodifiableMap(new HashMap<Integer, AdvancedExternalizer<?>>(advancedExternalizers));
      this.classResolver = classResolver;
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

   public ClassResolver classResolver() {
      return classResolver;
   }

   @Override
   public String toString() {
      return "SerializationConfiguration{" +
            "advancedExternalizers=" + advancedExternalizers +
            ", marshaller=" + marshaller +
            ", version=" + version +
            ", classResolver=" + classResolver +
            '}';
   }

}