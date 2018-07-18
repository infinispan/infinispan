package org.infinispan.marshall.core;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.UserObjectInput;
import org.infinispan.commons.marshall.UserObjectOutput;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

final class ExternalExternalizers {

   private static final Log log = LogFactory.getLog(ExternalExternalizers.class);

   private ExternalExternalizers() {
   }

   static ClassToExternalizerMap load(GlobalConfiguration globalCfg) {
      ClassToExternalizerMap exts = new ClassToExternalizerMap(4, 0.375f);

      Map<Integer, AdvancedExternalizer<?>> cfgExts = globalCfg.serialization().advancedExternalizers();
      for (Map.Entry<Integer, AdvancedExternalizer<?>> config : cfgExts.entrySet()) {
         AdvancedExternalizer ext = config.getValue();

         // If no XML or programmatic config, id in annotation is used
         // as long as it's not default one (meaning, user did not set it).
         // If XML or programmatic config in use ignore @Marshalls annotation and use value in config.
         Integer id = ext.getId();
         if (config.getKey() == null && id == null)
            throw new CacheConfigurationException(String.format(
                  "No advanced externalizer identifier set for externalizer %s",
                  ext.getClass().getName()));
         else if (config.getKey() != null)
            id = config.getKey();

         checkForeignIdLimit(id, ext);

         Set<Class> subTypes = ext.getTypeClasses();
         ForeignAdvancedExternalizer foreignExt = new ForeignAdvancedExternalizer(id, ext);
         for (Class<?> subType : subTypes)
            exts.put(subType, foreignExt);
      }

      return exts;
   }

   private static void checkForeignIdLimit(int id, AdvancedExternalizer<?> ext) {
      if (id < 0)
         throw log.foreignExternalizerUsingNegativeId(ext, id);
   }

   private static final class ForeignAdvancedExternalizer implements AdvancedExternalizer<Object> {

      final AdvancedExternalizer<Object> ext;
      final int id;

      private ForeignAdvancedExternalizer(int id, AdvancedExternalizer<Object> ext) {
         this.id = id;
         this.ext = ext;
      }

      @Override
      public Set<Class<?>> getTypeClasses() {
         return ext.getTypeClasses();
      }

      @Override
      public Integer getId() {
         return id;
      }

      @Override
      public void writeObject(UserObjectOutput output, Object object) throws IOException {
         ext.writeObject(output, object);
      }

      @Override
      public Object readObject(UserObjectInput input) throws IOException, ClassNotFoundException {
         return ext.readObject(input);
      }

      @Override
      public String toString() {
         // Each adapter is represented by the externalizer it delegates to, so just return the class name
         return ext.getClass().getName();
      }

   }

}
