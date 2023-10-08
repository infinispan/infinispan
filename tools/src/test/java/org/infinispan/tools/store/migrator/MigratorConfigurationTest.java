package org.infinispan.tools.store.migrator;

import static org.infinispan.tools.store.migrator.Element.ALLOW_LIST;
import static org.infinispan.tools.store.migrator.Element.BINARY;
import static org.infinispan.tools.store.migrator.Element.CACHE_NAME;
import static org.infinispan.tools.store.migrator.Element.CLASS;
import static org.infinispan.tools.store.migrator.Element.CLASSES;
import static org.infinispan.tools.store.migrator.Element.CONNECTION_POOL;
import static org.infinispan.tools.store.migrator.Element.CONNECTION_URL;
import static org.infinispan.tools.store.migrator.Element.CONTEXT_INITIALIZERS;
import static org.infinispan.tools.store.migrator.Element.DATA;
import static org.infinispan.tools.store.migrator.Element.DB;
import static org.infinispan.tools.store.migrator.Element.DIALECT;
import static org.infinispan.tools.store.migrator.Element.DISABLE_INDEXING;
import static org.infinispan.tools.store.migrator.Element.DISABLE_UPSERT;
import static org.infinispan.tools.store.migrator.Element.DRIVER_CLASS;
import static org.infinispan.tools.store.migrator.Element.EXTERNALIZERS;
import static org.infinispan.tools.store.migrator.Element.ID;
import static org.infinispan.tools.store.migrator.Element.MARSHALLER;
import static org.infinispan.tools.store.migrator.Element.NAME;
import static org.infinispan.tools.store.migrator.Element.REGEXPS;
import static org.infinispan.tools.store.migrator.Element.SEGMENT;
import static org.infinispan.tools.store.migrator.Element.SOURCE;
import static org.infinispan.tools.store.migrator.Element.STRING;
import static org.infinispan.tools.store.migrator.Element.TABLE;
import static org.infinispan.tools.store.migrator.Element.TABLE_NAME_PREFIX;
import static org.infinispan.tools.store.migrator.Element.TARGET;
import static org.infinispan.tools.store.migrator.Element.TIMESTAMP;
import static org.infinispan.tools.store.migrator.Element.TYPE;
import static org.infinispan.tools.store.migrator.Element.VERSION;
import static org.infinispan.tools.store.migrator.StoreType.JDBC_MIXED;
import static org.infinispan.tools.store.migrator.TestUtil.propKey;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.tools.store.migrator.marshaller.common.AdvancedExternalizer;
import org.infinispan.commons.marshall.JavaSerializationMarshaller;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.commons.test.ThreadLeakChecker;
import org.infinispan.commons.util.Version;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.AllowListConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.jboss.marshalling.commons.CheckedClassResolver;
import org.infinispan.jboss.marshalling.commons.GenericJBossMarshaller;
import org.infinispan.marshall.core.impl.DelegatingUserMarshaller;
import org.infinispan.marshall.persistence.PersistenceMarshaller;
import org.infinispan.persistence.jdbc.common.DatabaseType;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfiguration;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.infinispan.persistence.jdbc.impl.table.TableManagerFactory;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.data.Person;
import org.infinispan.tools.store.migrator.jdbc.JdbcConfigurationUtil;
import org.infinispan.tools.store.migrator.marshaller.SerializationConfigUtil;
import org.infinispan.tools.store.migrator.marshaller.infinispan10.Infinispan10Marshaller;
import org.infinispan.tools.store.migrator.marshaller.infinispan8.Infinispan8Marshaller;
import org.infinispan.tools.store.migrator.marshaller.infinispan9.Infinispan9Marshaller;
import org.jboss.marshalling.MarshallingConfiguration;
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
      // Ignore all threads, SerializationConfigUtil.getMarshaller() starts a cache manager and doesn't stop it
      ThreadLeakChecker.ignoreThreadsContaining("");
      externalizerReadCount.set(0);
      externalizerWriteCount.set(0);
   }

   public void testAllowListLoadedForTargetStores() {
      Properties properties = createBaseProperties(TARGET);
      properties.put(propKey(TARGET, MARSHALLER, CLASS), JavaSerializationMarshaller.class.getName());
      properties.put(propKey(TARGET, MARSHALLER, ALLOW_LIST, CLASSES), "org.example.Person,org.example.Animal");
      properties.put(propKey(TARGET, MARSHALLER, ALLOW_LIST, REGEXPS), "org.another.example.*");

      GlobalConfigurationBuilder builder = new GlobalConfigurationBuilder();
      StoreProperties storeProps = new StoreProperties(TARGET, properties);
      SerializationConfigUtil.configureSerialization(storeProps, builder.serialization());

      AllowListConfiguration allowList = builder.build().serialization().allowList();
      assertEquals(allowList.getClasses(), Set.of("org.example.Person", "org.example.Animal"));
      assertEquals(allowList.getRegexps(), Set.of("org.another.example.*"));
   }

   public void testCustomMarshallerLoadedLegacy() {
      Properties properties = createBaseProperties();
      properties.put(propKey(SOURCE, VERSION), String.valueOf(8));
      properties.put(propKey(SOURCE, MARSHALLER, CLASS), GenericJBossMarshaller.class.getName());
      properties.put(propKey(SOURCE, MARSHALLER, ALLOW_LIST, CLASSES), "org.example.Person,org.example.Animal");
      properties.put(propKey(SOURCE, MARSHALLER, ALLOW_LIST, REGEXPS), "org.another.example.*");

      StoreProperties props = new StoreProperties(SOURCE, properties);
      Marshaller jBossMarshaller = SerializationConfigUtil.getMarshaller(props);
      assertNotNull(jBossMarshaller);
      assertTrue(jBossMarshaller instanceof GenericJBossMarshaller);
      MarshallingConfiguration config = TestingUtil.extractField(jBossMarshaller, "baseCfg");
      CheckedClassResolver classResolver = (CheckedClassResolver) config.getClassResolver();
      ClassAllowList allowList = TestingUtil.extractField(classResolver, "classAllowList");
      assertTrue(allowList.isSafeClass("org.example.Person"));
      assertTrue(allowList.isSafeClass("org.example.Animal"));
      assertTrue(allowList.isSafeClass("org.another.example.Person"));
   }

   public void testCustomMarshallerLoaded() {
      Properties properties = createBaseProperties();
      properties.put(propKey(SOURCE, MARSHALLER, CLASS), GenericJBossMarshaller.class.getName());
      properties.put(propKey(SOURCE, MARSHALLER, ALLOW_LIST, CLASSES), "org.example.Person,org.example.Animal");
      properties.put(propKey(SOURCE, MARSHALLER, ALLOW_LIST, REGEXPS), "org.another.example.*");

      StoreProperties props = new StoreProperties(SOURCE, properties);
      Marshaller marshaller = SerializationConfigUtil.getMarshaller(props);
      assertNotNull(marshaller);
      assertTrue(marshaller instanceof PersistenceMarshaller);
      PersistenceMarshaller pm = (PersistenceMarshaller) marshaller;
      DelegatingUserMarshaller userMarshaller = (DelegatingUserMarshaller) pm.getUserMarshaller();
      assertTrue(userMarshaller.getDelegate() instanceof GenericJBossMarshaller);
      GenericJBossMarshaller jBossMarshaller = (GenericJBossMarshaller) userMarshaller.getDelegate();
      MarshallingConfiguration config = TestingUtil.extractField(jBossMarshaller, "baseCfg");
      CheckedClassResolver classResolver = (CheckedClassResolver) config.getClassResolver();
      ClassAllowList allowList = TestingUtil.extractField(classResolver, "classAllowList");
      assertTrue(allowList.isSafeClass("org.example.Person"));
      assertTrue(allowList.isSafeClass("org.example.Animal"));
      assertTrue(allowList.isSafeClass("org.another.example.Person"));
   }

   public void testInfinipsan8MarshallerAndExternalizersLoaded() throws Exception {
      String externalizers = String.format("%d:%s", 1, PersonExternalizer.class.getName());
      Properties properties = createBaseProperties();
      properties.put(propKey(SOURCE, VERSION), String.valueOf(8));
      properties.put(propKey(SOURCE, MARSHALLER, EXTERNALIZERS), externalizers);

      StoreProperties props = new StoreProperties(SOURCE, properties);
      Marshaller marshaller = SerializationConfigUtil.getMarshaller(props);
      assertNotNull(marshaller);
      assertTrue(marshaller instanceof Infinispan8Marshaller);

      byte[] bytes = new byte[] {3, 1, -2, 3, -1, 1, 1};
      Object object = marshaller.objectFromByteBuffer(bytes);
      assertNotNull(object);
      assertTrue(object instanceof Person);
      assertEquals(1, externalizerReadCount.get());
   }

   public void testInfinispan9MarshallerLoadedAndExternalizersLoaded() throws Exception {
      String externalizers = String.format("%d:%s", 1, PersonExternalizer.class.getName());
      Properties properties = createBaseProperties();
      properties.put(propKey(SOURCE, VERSION), String.valueOf(9));
      properties.put(propKey(SOURCE, MARSHALLER, EXTERNALIZERS), externalizers);

      StoreProperties props = new StoreProperties(SOURCE, properties);
      Marshaller marshaller = SerializationConfigUtil.getMarshaller(props);
      assertNotNull(marshaller);
      assertTrue(marshaller instanceof Infinispan9Marshaller);

      byte[] bytes = new byte[] {3, 0, 0, 0, 1, 1};
      Object object = marshaller.objectFromByteBuffer(bytes);
      assertNotNull(object);
      assertTrue(object instanceof Person);
      assertEquals(1, externalizerReadCount.get());
   }

   public void testCurrentMarshallerLoadedAndSCILoaded() throws Exception {
      Properties properties = createBaseProperties();
      properties.put(propKey(SOURCE, MARSHALLER, CONTEXT_INITIALIZERS), TestUtil.SCI.INSTANCE.getClass().getName());

      StoreProperties props = new StoreProperties(SOURCE, properties);
      Marshaller marshaller = SerializationConfigUtil.getMarshaller(props);
      assertNotNull(marshaller);
      assertTrue(marshaller instanceof PersistenceMarshaller);
      byte[] bytes = marshaller.objectToByteBuffer(new Person(Person.class.getName()));
      Person person = (Person) marshaller.objectFromByteBuffer(bytes);
      assertNotNull(person);
      assertEquals(Person.class.getName(), person.getName());
   }

   public void testExceptionOnMarshallerType() {
      Properties properties = createBaseProperties();
      properties.put(propKey(SOURCE, MARSHALLER, TYPE), "CURRENT");
      Exceptions.expectException(CacheConfigurationException.class, () -> new StoreProperties(SOURCE, properties));
   }

   public void testCorrectMarshallerLoadedForVersion() {
      assertTrue(getMarshallerForVersion(8, SOURCE) instanceof Infinispan8Marshaller);
      assertTrue(getMarshallerForVersion(9, SOURCE) instanceof Infinispan9Marshaller);
      assertTrue(getMarshallerForVersion(10, SOURCE) instanceof Infinispan10Marshaller);
      assertTrue(getMarshallerForVersion(11, SOURCE) instanceof Infinispan10Marshaller);
      assertTrue(getMarshallerForVersion(12, SOURCE) instanceof PersistenceMarshaller);
      assertTrue(getMarshallerForVersion(13, SOURCE) instanceof PersistenceMarshaller);

      Exceptions.expectException(CacheConfigurationException.class, () -> getMarshallerForVersion(8, TARGET));
      Exceptions.expectException(CacheConfigurationException.class, () -> getMarshallerForVersion(9, TARGET));
      Exceptions.expectException(CacheConfigurationException.class, () -> getMarshallerForVersion(10, TARGET));
      Exceptions.expectException(CacheConfigurationException.class, () -> getMarshallerForVersion(11, TARGET));
      Exceptions.expectException(CacheConfigurationException.class, () -> getMarshallerForVersion(12, TARGET));
      assertNull(getMarshallerForVersion(Integer.parseInt(Version.getMajor()), TARGET));
   }

   private Marshaller getMarshallerForVersion(int version, Element storeType) {
      Properties properties = createBaseProperties(storeType);
      properties.put(propKey(storeType, VERSION), String.valueOf(version));
      StoreProperties props = new StoreProperties(storeType, properties);
      return SerializationConfigUtil.getMarshaller(props);
   }

   public void testDbPropertiesLoaded() {
      Properties properties = createBaseProperties();
      properties.putAll(createBaseProperties(TARGET));
      Element[] storeTypes = new Element[] {SOURCE, TARGET};
      for (Element storeType : storeTypes) {
         properties.put(propKey(storeType, DB, DISABLE_INDEXING), "true");
         properties.put(propKey(storeType, DB, DISABLE_UPSERT), "true");

         for (Element store : Arrays.asList(STRING, BINARY)) {
            properties.put(propKey(storeType, TABLE, store, TABLE_NAME_PREFIX), "mock_table_name");
            properties.put(propKey(storeType, TABLE, store, ID, NAME), "mock_id_column_name");
            properties.put(propKey(storeType, TABLE, store, ID, TYPE), "mock_id_column_type");
            properties.put(propKey(storeType, TABLE, store, DATA, NAME), "mock_data_column_name");
            properties.put(propKey(storeType, TABLE, store, DATA, TYPE), "mock_data_column_type");
            properties.put(propKey(storeType, TABLE, store, TIMESTAMP, NAME), "mock_timestamp_column_name");
            properties.put(propKey(storeType, TABLE, store, TIMESTAMP, TYPE), "mock_timestamp_column_type");
            properties.put(propKey(storeType, TABLE, store, SEGMENT, NAME), "mock_segment_column_name");
            properties.put(propKey(storeType, TABLE, store, SEGMENT, TYPE), "mock_segment_column_type");
         }
      }

      for (Element storeType : storeTypes) {
         StoreProperties props = new StoreProperties(storeType, properties);
         JdbcStringBasedStoreConfigurationBuilder builder = new ConfigurationBuilder().persistence()
               .addStore(JdbcStringBasedStoreConfigurationBuilder.class);
         Configuration cacheConfig = JdbcConfigurationUtil.configureStore(props, builder).build();
         JdbcStringBasedStoreConfiguration config = (JdbcStringBasedStoreConfiguration) cacheConfig.persistence().stores().get(0);
         assertNull(config.dbMajorVersion());
         assertNull(config.dbMinorVersion());
         assertTrue(Boolean.parseBoolean(config.properties().getProperty(TableManagerFactory.INDEXING_DISABLED)));
         assertTrue(Boolean.parseBoolean(config.properties().getProperty(TableManagerFactory.UPSERT_DISABLED)));
      }
   }

   private Properties createBaseProperties() {
      return createBaseProperties(SOURCE);
   }

   private Properties createBaseProperties(Element orientation) {
      Properties properties = new Properties();
      properties.put(propKey(orientation, CACHE_NAME), DEFAULT_CACHE_NAME);
      properties.put(propKey(orientation, TYPE), JDBC_MIXED.toString());
      properties.put(propKey(orientation, DIALECT), DatabaseType.H2.toString());
      properties.put(propKey(orientation, CONNECTION_POOL, CONNECTION_URL), "jdbc:postgresql:postgres");
      properties.put(propKey(orientation, CONNECTION_POOL, DRIVER_CLASS), "org.postgresql.Driver");
      return properties;
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
         return new Person(MarshallUtil.unmarshallString(input));
      }
   }
}
