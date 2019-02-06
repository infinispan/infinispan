package org.infinispan.tools.store.migrator.marshaller;

import static org.infinispan.tools.store.migrator.Element.CLASS;
import static org.infinispan.tools.store.migrator.Element.EXTERNALIZERS;
import static org.infinispan.tools.store.migrator.Element.MARSHALLER;
import static org.infinispan.tools.store.migrator.Element.SOURCE;

import java.util.HashMap;
import java.util.Map;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.global.SerializationConfigurationBuilder;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.persistence.PersistenceMarshaller;
import org.infinispan.marshall.persistence.impl.MarshalledEntryFactoryImpl;
import org.infinispan.persistence.spi.MarshallableEntryFactory;
import org.infinispan.tools.store.migrator.StoreProperties;
import org.infinispan.tools.store.migrator.marshaller.infinispan8.Infinispan8Marshaller;
import org.infinispan.tools.store.migrator.marshaller.infinispan9.Infinispan9Marshaller;

public class SerializationConfigUtil {

   public static void configureSerialization(StoreProperties props, SerializationConfigurationBuilder builder) {
      Marshaller marshaller = getMarshaller(props);
      builder.marshaller(marshaller);
      configureExternalizers(props, builder);
   }

   public static MarshallableEntryFactory getEntryFactory(StoreProperties props) {
      return getEntryFactory(getMarshaller(props));
   }

   public static MarshallableEntryFactory getEntryFactory(Marshaller marshaller) {
      return new MarshalledEntryFactoryImpl(marshaller);
   }

   public static Marshaller getMarshaller(StoreProperties props) {
      String marshallerClass = props.get(MARSHALLER, CLASS);
      if (marshallerClass != null) {
         try {
            return (Marshaller) Util.loadClass(marshallerClass, SerializationConfigUtil.class.getClassLoader()).newInstance();
         } catch (IllegalAccessException | InstantiationException e) {
            throw new CacheConfigurationException(String.format("Unable to load StreamingMarshaller '%s' for %s store",
                  marshallerClass, SOURCE), e);
         }
      }

      int majorVersion = props.getMajorVersion();
      switch (majorVersion) {
         case 8:
         case 9:
            if (props.isTargetStore())
               throw new CacheConfigurationException(String.format("The marshaller associated with Infinispan %d can only be specified for source stores.", majorVersion));
            Map<Integer, AdvancedExternalizer> userExts = getExternalizersFromProps(props);
            return majorVersion == 8 ? new Infinispan8Marshaller(userExts) : new Infinispan9Marshaller(userExts);
         case 10:
            if (props.isTargetStore())
               return null;

            GlobalConfigurationBuilder globalConfig = new GlobalConfigurationBuilder()
                  .defaultCacheName(props.cacheName());
            configureExternalizers(props, globalConfig.serialization());

            EmbeddedCacheManager manager = new DefaultCacheManager(globalConfig.build(), new ConfigurationBuilder().build());
            return manager.getCache().getAdvancedCache().getComponentRegistry().getComponent(PersistenceMarshaller.class, KnownComponentNames.PERSISTENCE_MARSHALLER);
         default:
            throw new IllegalStateException(String.format("Unexpected major version '%d'", majorVersion));
      }
   }

   private static void configureExternalizers(StoreProperties props, SerializationConfigurationBuilder builder) {
      Map<Integer, AdvancedExternalizer> externalizerMap = getExternalizersFromProps(props);
      if (externalizerMap == null)
         return;

      for (Map.Entry<Integer, AdvancedExternalizer> entry : externalizerMap.entrySet())
         builder.addAdvancedExternalizer(entry.getKey(), entry.getValue());
   }

   // Expects externalizer string to be a comma-separated list of "<id>:<class>"
   private static Map<Integer, AdvancedExternalizer> getExternalizersFromProps(StoreProperties props) {
      Map<Integer, AdvancedExternalizer> map = new HashMap<>();
      String externalizers = props.get(MARSHALLER, EXTERNALIZERS);
      if (externalizers != null) {
         for (String ext : externalizers.split(",")) {
            String[] extArray = ext.split(":");
            String className = extArray.length > 1 ? extArray[1] : extArray[0];
            AdvancedExternalizer<?> instance = Util.getInstance(className, SerializationConfigUtil.class.getClassLoader());
            int id = extArray.length > 1 ? new Integer(extArray[0]) : instance.getId();
            map.put(id, instance);
         }
      }
      return map;
   }
}
