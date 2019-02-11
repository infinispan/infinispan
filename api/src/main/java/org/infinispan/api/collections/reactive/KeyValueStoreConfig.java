package org.infinispan.api.collections.reactive;

public class KeyValueStoreConfig {

   private static final String SCHEMA_EXTENSION = ".proto";
   private Class valueClazz;
   private String packageName;
   private String schemaFileName;

   private KeyValueStoreConfig() {

   }

   public static KeyValueStoreConfig init(Class valueClazz) {
      KeyValueStoreConfig keyValueStoreConfig = new KeyValueStoreConfig();
      keyValueStoreConfig.valueClazz = valueClazz;
      keyValueStoreConfig.schemaFileName = valueClazz.getName() + SCHEMA_EXTENSION;
      keyValueStoreConfig.packageName = valueClazz.getPackage().getName();
      return keyValueStoreConfig;
   }

   public KeyValueStoreConfig withPackageName(String packageName) {
      this.packageName = packageName;
      return this;
   }

   public KeyValueStoreConfig withSchemaFileName(String fileName) {
      this.schemaFileName = fileName + SCHEMA_EXTENSION;
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
}
