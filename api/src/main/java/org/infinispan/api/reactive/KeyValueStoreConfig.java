package org.infinispan.api.reactive;

import java.util.HashSet;
import java.util.Set;

import org.infinispan.api.marshalling.Marshaller;

/**
 * Configure the key value store
 *
 * @since 10.0
 */
public class KeyValueStoreConfig {

   private static final String SCHEMA_EXTENSION = ".proto";
   private Class valueClazz;
   private String packageName;
   private String schemaFileName;
   private Set<Marshaller> marshallers;

   private KeyValueStoreConfig() {

   }

   public static KeyValueStoreConfig init(Class valueClazz) {
      KeyValueStoreConfig keyValueStoreConfig = new KeyValueStoreConfig();
      keyValueStoreConfig.valueClazz = valueClazz;
      keyValueStoreConfig.schemaFileName = valueClazz.getName() + SCHEMA_EXTENSION;
      keyValueStoreConfig.packageName = valueClazz.getPackage().getName();
      keyValueStoreConfig.marshallers = new HashSet<>();
      return keyValueStoreConfig;
   }

   public static KeyValueStoreConfig defaultConfig() {
      return new KeyValueStoreConfig();
   }

   public KeyValueStoreConfig withSchemaFileName(String fileName) {
      this.schemaFileName = fileName + SCHEMA_EXTENSION;
      return this;
   }

   public KeyValueStoreConfig withPackageName(String packageName) {
      this.packageName = packageName;
      return this;
   }

   // Does not work, TODO https://issues.jboss.org/browse/ISPN-9973
   public KeyValueStoreConfig addMarshaller(Marshaller marshaller) {
      marshallers.add(marshaller);
      return this;
   }

   public Class getValueClazz() {
      return valueClazz;
   }

   public String getPackageName() {
      return packageName;
   }

   public String getSchemaFileName() {
      return schemaFileName;
   }

   public Set<Marshaller> getMarshallers() {
      return marshallers;
   }
}
