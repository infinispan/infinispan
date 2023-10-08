package org.infinispan.tools.store.migrator.marshaller;

import static org.infinispan.commons.util.Util.EMPTY_STRING_ARRAY;
import static org.infinispan.tools.store.migrator.Element.ALLOW_LIST;
import static org.infinispan.tools.store.migrator.Element.CLASS;
import static org.infinispan.tools.store.migrator.Element.CLASSES;
import static org.infinispan.tools.store.migrator.Element.CONTEXT_INITIALIZERS;
import static org.infinispan.tools.store.migrator.Element.EXTERNALIZERS;
import static org.infinispan.tools.store.migrator.Element.MARSHALLER;
import static org.infinispan.tools.store.migrator.Element.REGEXPS;
import static org.infinispan.tools.store.migrator.Element.SOURCE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.infinispan.Cache;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.Version;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.global.SerializationConfigurationBuilder;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.persistence.PersistenceMarshaller;
import org.infinispan.marshall.persistence.impl.MarshalledEntryFactoryImpl;
import org.infinispan.persistence.spi.MarshallableEntryFactory;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.tools.store.migrator.StoreProperties;
import org.infinispan.tools.store.migrator.marshaller.common.AdvancedExternalizer;
import org.infinispan.tools.store.migrator.marshaller.infinispan10.Infinispan10Marshaller;
import org.infinispan.tools.store.migrator.marshaller.infinispan8.Infinispan8Marshaller;
import org.infinispan.tools.store.migrator.marshaller.infinispan9.Infinispan9Marshaller;

public class SerializationConfigUtil {

   public static void configureSerialization(StoreProperties props, SerializationConfigurationBuilder builder) {
      Marshaller marshaller = getMarshaller(props);
      builder.marshaller(marshaller);
      configureAllowList(props, builder);
      configureSerializationContextInitializers(props, builder);
   }

   public static MarshallableEntryFactory getEntryFactory(StoreProperties props) {
      return getEntryFactory(getMarshaller(props));
   }

   public static MarshallableEntryFactory getEntryFactory(Marshaller marshaller) {
      return new MarshalledEntryFactoryImpl(marshaller);
   }

   public static Marshaller getMarshaller(StoreProperties props) {
      int majorVersion = props.getMajorVersion();
      if (props.isTargetStore() && majorVersion != Integer.parseInt(Version.getMajor())) {
         throw new CacheConfigurationException(String.format("The marshaller associated with Infinispan %d can only be specified for source stores.", majorVersion));
      }

      if (majorVersion < 8 || majorVersion > Integer.parseInt(Version.getMajor())) {
         throw new IllegalStateException(String.format("Unexpected major version '%d'", majorVersion));
      }

      Marshaller marshaller;
      switch (majorVersion) {
         case 8:
         case 9:
            marshaller = loadMarshallerInstance(props);
            if (marshaller != null) {
               return marshaller;
            }

            Map<Integer, AdvancedExternalizer> userExts = getExternalizersFromProps(props);
            return majorVersion == 8 ? new Infinispan8Marshaller(userExts) : new Infinispan9Marshaller(userExts);
         case 10:
         case 11:
            marshaller = loadMarshallerInstance(props);
            return new Infinispan10Marshaller(marshaller, getSCIClasses(props));
         default:
            return props.isTargetStore() ? null : createPersistenceMarshaller(props);
      }
   }

   private static PersistenceMarshaller createPersistenceMarshaller(StoreProperties props) {
      GlobalConfigurationBuilder globalConfig = new GlobalConfigurationBuilder();
      configureAllowList(props, globalConfig.serialization());
      configureSerializationContextInitializers(props, globalConfig.serialization());
      Marshaller marshaller = loadMarshallerInstance(props);
      if (marshaller != null) {
         globalConfig.serialization().marshaller(marshaller);
      }

      EmbeddedCacheManager manager = new DefaultCacheManager(globalConfig.build());
      try {
         Cache<Object, Object> cache = manager.createCache(props.cacheName(), new ConfigurationBuilder().build());
         return ComponentRegistry.componentOf(cache, PersistenceMarshaller.class, KnownComponentNames.PERSISTENCE_MARSHALLER);
      } finally {
         manager.stop();
      }
   }

   private static Marshaller loadMarshallerInstance(StoreProperties props) {
      String marshallerClass = props.get(MARSHALLER, CLASS);
      if (marshallerClass != null) {
         try {
            Marshaller marshaller = (Marshaller) Util.loadClass(marshallerClass, SerializationConfigUtil.class.getClassLoader()).newInstance();
            ClassAllowList allowList = new ClassAllowList();
            allowList.addClasses(allowClasses(props));
            allowList.addRegexps(allowRegexps(props));
            marshaller.initialize(allowList);
            return marshaller;
         } catch (IllegalAccessException | InstantiationException e) {
            throw new CacheConfigurationException(String.format("Unable to load StreamingMarshaller '%s' for %s store",
                  marshallerClass, SOURCE), e);
         }
      }
      return null;
   }

   private static void configureAllowList(StoreProperties props, SerializationConfigurationBuilder builder) {
      builder.allowList()
              .addClasses(allowClasses(props))
              .addRegexps(allowRegexps(props));
   }

   private static String[] allowClasses(StoreProperties props) {
      return propToArray(props.get(MARSHALLER, ALLOW_LIST, CLASSES));
   }

   private static String[] allowRegexps(StoreProperties props) {
      return propToArray(props.get(MARSHALLER, ALLOW_LIST, REGEXPS));
   }

   private static String[] propToArray(String prop) {
      if (prop != null)
         return prop.split(",");

      return EMPTY_STRING_ARRAY;
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
            int id = extArray.length > 1 ? Integer.parseInt(extArray[0]) : instance.getId();
            map.put(id, instance);
         }
      }
      return map;
   }

   private static void configureSerializationContextInitializers(StoreProperties props, SerializationConfigurationBuilder builder) {
      getSCIClasses(props).forEach(builder::addContextInitializer);
   }

   public static List<SerializationContextInitializer> getSCIClasses(StoreProperties props) {
      String sciString = props.get(MARSHALLER, CONTEXT_INITIALIZERS);
      if (sciString == null)
         return new ArrayList<>();

      return Arrays.stream(sciString.split(","))
            .map(impl -> Util.<SerializationContextInitializer>getInstance(impl, SerializationConfigUtil.class.getClassLoader()))
            .collect(Collectors.toList());
   }
}
