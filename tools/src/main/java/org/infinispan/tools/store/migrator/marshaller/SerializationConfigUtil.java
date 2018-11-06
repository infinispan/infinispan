package org.infinispan.tools.store.migrator.marshaller;

import static org.infinispan.tools.store.migrator.Element.CLASS;
import static org.infinispan.tools.store.migrator.Element.EXTERNALIZERS;
import static org.infinispan.tools.store.migrator.Element.MARSHALLER;
import static org.infinispan.tools.store.migrator.Element.SOURCE;
import static org.infinispan.tools.store.migrator.Element.TYPE;

import java.util.HashMap;
import java.util.Map;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.global.SerializationConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.persistence.impl.MarshalledEntryFactoryImpl;
import org.infinispan.persistence.spi.MarshalledEntryFactory;
import org.infinispan.tools.store.migrator.StoreProperties;

public class SerializationConfigUtil {

   public static void configureSerialization(StoreProperties props, SerializationConfigurationBuilder builder) {
      Marshaller marshaller = getMarshaller(props);
      builder.marshaller(marshaller);
      configureExternalizers(props, builder);
   }

   public static MarshalledEntryFactory getEntryFactory(StoreProperties props) {
      return getEntryFactory(getMarshaller(props));
   }

   public static MarshalledEntryFactory getEntryFactory(Marshaller marshaller) {
      return new MarshalledEntryFactoryImpl(marshaller);
   }

   public static StreamingMarshaller getMarshaller(StoreProperties props) {
      switch (getMarshallerType(props)) {
         case CURRENT:
            if (props.isTargetStore())
               return null;

            GlobalConfigurationBuilder globalConfig = new GlobalConfigurationBuilder()
                  .defaultCacheName(props.cacheName());
            configureExternalizers(props, globalConfig.serialization());

            EmbeddedCacheManager manager = new DefaultCacheManager(globalConfig.build(), new ConfigurationBuilder().build());
            return manager.getCache().getAdvancedCache().getComponentRegistry().getComponent(StreamingMarshaller.class);
         case CUSTOM:
            String marshallerClass = props.get(MARSHALLER, CLASS);
            if (marshallerClass == null)
               throw new CacheConfigurationException(
                     String.format("The property %s.%s must be set if a custom marshaller type is specified", MARSHALLER, CLASS));

            try {
               return (StreamingMarshaller) Util.loadClass(marshallerClass, SerializationConfigUtil.class.getClassLoader()).newInstance();
            } catch (IllegalAccessException | InstantiationException e) {
               throw new CacheConfigurationException(String.format("Unable to load StreamingMarshaller '%s' for %s store",
                     marshallerClass, SOURCE), e);
            }
         case LEGACY:
            if (props.isTargetStore())
               throw new CacheConfigurationException("The legacy marshaller can only be specified for source stores.");
            return new LegacyVersionAwareMarshaller(getExternalizersFromProps(props));
         default:
            throw new IllegalStateException("Unexpected marshaller type");
      }
   }

   private static MarshallerType getMarshallerType(StoreProperties props) {
      MarshallerType marshallerType = MarshallerType.CURRENT;
      String marshallerTypeProp = props.get(MARSHALLER, TYPE);
      if (marshallerTypeProp != null)
         marshallerType = MarshallerType.valueOf(props.get(MARSHALLER, TYPE).toUpperCase());
      return marshallerType;
   }

   private static void configureExternalizers(StoreProperties props, SerializationConfigurationBuilder builder) {
      Map<Integer, AdvancedExternalizer<?>> externalizerMap = getExternalizersFromProps(props);
      if (externalizerMap == null)
         return;

      for (Map.Entry<Integer, AdvancedExternalizer<?>> entry : externalizerMap.entrySet())
         builder.addAdvancedExternalizer(entry.getKey(), entry.getValue());
   }

   // Expects externalizer string to be a comma-separated list of "<id>:<class>"
   private static Map<Integer, AdvancedExternalizer<?>> getExternalizersFromProps(StoreProperties props) {
      Map<Integer, AdvancedExternalizer<?>> map = new HashMap<>();
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
