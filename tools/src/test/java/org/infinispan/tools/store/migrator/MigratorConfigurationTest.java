package org.infinispan.tools.store.migrator;

import static org.infinispan.tools.store.migrator.Element.CACHE_NAME;
import static org.infinispan.tools.store.migrator.Element.CLASS;
import static org.infinispan.tools.store.migrator.Element.CONNECTION_POOL;
import static org.infinispan.tools.store.migrator.Element.CONNECTION_URL;
import static org.infinispan.tools.store.migrator.Element.DB;
import static org.infinispan.tools.store.migrator.Element.DIALECT;
import static org.infinispan.tools.store.migrator.Element.DISABLE_INDEXING;
import static org.infinispan.tools.store.migrator.Element.DISABLE_UPSERT;
import static org.infinispan.tools.store.migrator.Element.DRIVER_CLASS;
import static org.infinispan.tools.store.migrator.Element.EXTERNALIZERS;
import static org.infinispan.tools.store.migrator.Element.MAJOR_VERSION;
import static org.infinispan.tools.store.migrator.Element.MARSHALLER;
import static org.infinispan.tools.store.migrator.Element.MINOR_VERSION;
import static org.infinispan.tools.store.migrator.Element.SOURCE;
import static org.infinispan.tools.store.migrator.Element.TARGET;
import static org.infinispan.tools.store.migrator.Element.TYPE;
import static org.infinispan.tools.store.migrator.StoreType.MIXED;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.marshall.jboss.GenericJBossMarshaller;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.marshall.core.GlobalMarshaller;
import org.infinispan.persistence.jdbc.DatabaseType;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfiguration;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.infinispan.persistence.jdbc.table.management.TableManagerFactory;
import org.infinispan.test.data.Person;
import org.infinispan.tools.store.migrator.jdbc.JdbcConfigurationUtil;
import org.infinispan.tools.store.migrator.marshaller.LegacyVersionAwareMarshaller;
import org.infinispan.tools.store.migrator.marshaller.MarshallerType;
import org.infinispan.tools.store.migrator.marshaller.SerializationConfigUtil;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * @author Ryan Emerson
 * @since 9.0
 */
@Test(testName = "tools.MigratorConfigurationTest", groups = "functional")
public class MigratorConfigurationTest {

   private static final String DEFAULT_CACHE_NAME = "testCache";
   private static final AtomicInteger externalizerReadCount = new AtomicInteger();
   private static final AtomicInteger externalizerWriteCount = new AtomicInteger();

   @BeforeMethod
   public void init() {
      externalizerReadCount.set(0);
      externalizerWriteCount.set(0);
   }

   public void testCustomMarshallerLoaded() {
      Properties properties = createBaseProperties();
      properties.put(propKey(SOURCE, MARSHALLER, TYPE), MarshallerType.CUSTOM.toString());
      properties.put(propKey(SOURCE, MARSHALLER, CLASS), GenericJBossMarshaller.class.getName());

      StoreProperties props = new StoreProperties(SOURCE, properties);
      StreamingMarshaller marshaller = SerializationConfigUtil.getMarshaller(props);
      assert marshaller != null;
      assert marshaller instanceof GenericJBossMarshaller;
   }

   public void testLegacyMarshallerAndExternalizersLoaded() throws Exception {
      String externalizers = "1:" + PersonExternalizer.class.getName();
      Properties properties = createBaseProperties();
      properties.put(propKey(SOURCE, MARSHALLER, TYPE), MarshallerType.LEGACY.toString());
      properties.put(propKey(SOURCE, MARSHALLER, EXTERNALIZERS), externalizers);

      StoreProperties props = new StoreProperties(SOURCE, properties);
      StreamingMarshaller marshaller = SerializationConfigUtil.getMarshaller(props);
      assert marshaller != null;
      assert marshaller instanceof LegacyVersionAwareMarshaller;

      byte[] bytes = new byte[] {3, 1, -2, 3, -1, 1, 1};
      Object object = marshaller.objectFromByteBuffer(bytes);
      assert object != null;
      assert object instanceof Person;
      assert externalizerReadCount.get() == 1;
   }

   public void testCurrentMarshallerLoadedAndExternalizersLoaded() throws Exception {
      String externalizers = "1:" + PersonExternalizer.class.getName();
      Properties properties = createBaseProperties();
      properties.put(propKey(SOURCE, MARSHALLER, TYPE), MarshallerType.CURRENT.toString());
      properties.put(propKey(SOURCE, MARSHALLER, EXTERNALIZERS), externalizers);

      StoreProperties props = new StoreProperties(SOURCE, properties);
      StreamingMarshaller marshaller = SerializationConfigUtil.getMarshaller(props);
      assert marshaller != null;
      assert marshaller instanceof GlobalMarshaller;
      byte[] bytes = marshaller.objectToByteBuffer(new Person(Person.class.getName()));
      Person person = (Person) marshaller.objectFromByteBuffer(bytes);
      assert person != null;
      assert person.getName().equals(Person.class.getName());
      assert externalizerReadCount.get() == 1;
      assert externalizerWriteCount.get() == 1;
   }

   public void testDbPropertiesLoaded() {
      Properties properties = createBaseProperties();
      properties.putAll(createBaseProperties(TARGET));
      Element[] storeTypes = new Element[] {SOURCE, TARGET};
      for (Element storeType : storeTypes) {
         properties.put(propKey(storeType, DB, MAJOR_VERSION), "1");
         properties.put(propKey(storeType, DB, MINOR_VERSION), "1");
         properties.put(propKey(storeType, DB, DISABLE_INDEXING), "true");
         properties.put(propKey(storeType, DB, DISABLE_UPSERT), "true");
      }

      for (Element storeType : storeTypes) {
         StoreProperties props = new StoreProperties(storeType, properties);
         JdbcStringBasedStoreConfigurationBuilder builder = new ConfigurationBuilder().persistence()
               .addStore(JdbcStringBasedStoreConfigurationBuilder.class);
         builder = JdbcConfigurationUtil.configureStore(props, builder);
         Configuration cacheConfig = builder.build();
         JdbcStringBasedStoreConfiguration config = (JdbcStringBasedStoreConfiguration) cacheConfig.persistence().stores().get(0);
         assert config.dbMajorVersion() == 1;
         assert config.dbMinorVersion() == 1;
         assert Boolean.parseBoolean(config.properties().getProperty(TableManagerFactory.INDEXING_DISABLED));
         assert Boolean.parseBoolean(config.properties().getProperty(TableManagerFactory.UPSERT_DISABLED));
      }

   }

   private Properties createBaseProperties() {
      return createBaseProperties(SOURCE);
   }

   private Properties createBaseProperties(Element orientation) {
      Properties properties = new Properties();
      properties.put(propKey(orientation, CACHE_NAME), DEFAULT_CACHE_NAME);
      properties.put(propKey(orientation, TYPE), MIXED.toString());
      properties.put(propKey(orientation, DIALECT), DatabaseType.H2.toString());
      properties.put(propKey(orientation, CONNECTION_POOL, CONNECTION_URL), "jdbc:postgresql:postgres");
      properties.put(propKey(orientation, CONNECTION_POOL, DRIVER_CLASS), "org.postgresql.Driver");
      return properties;
   }

   private String propKey(Element... elements) {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < elements.length; i++) {
         sb.append(elements[i].toString());
         if (i != elements.length - 1) sb.append(".");
      }
      return sb.toString();
   }

   public static class PersonExternalizer implements AdvancedExternalizer<Person> {
      @Override
      public Set<Class<? extends Person>> getTypeClasses() {
         return Collections.singleton(Person.class);
      }

      @Override
      public void writeObject(ObjectOutput output, Person object) throws IOException {
         externalizerWriteCount.incrementAndGet();
         MarshallUtil.marshallString(object.getName(), output);
      }

      @Override
      public Integer getId() {
         return 1;
      }

      @Override
      public Person readObject(ObjectInput input) throws IOException {
         externalizerReadCount.incrementAndGet();
         Person person = new Person();
         person.setName(MarshallUtil.unmarshallString(input));
         return person;
      }
   }
}
