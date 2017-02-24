package org.infinispan.tools.jdbc.migrator;

import static org.infinispan.tools.jdbc.migrator.Element.CACHE_NAME;
import static org.infinispan.tools.jdbc.migrator.Element.CLASS;
import static org.infinispan.tools.jdbc.migrator.Element.CONNECTION_POOL;
import static org.infinispan.tools.jdbc.migrator.Element.CONNECTION_URL;
import static org.infinispan.tools.jdbc.migrator.Element.DIALECT;
import static org.infinispan.tools.jdbc.migrator.Element.DRIVER_CLASS;
import static org.infinispan.tools.jdbc.migrator.Element.EXTERNALIZERS;
import static org.infinispan.tools.jdbc.migrator.Element.MARSHALLER;
import static org.infinispan.tools.jdbc.migrator.Element.SOURCE;
import static org.infinispan.tools.jdbc.migrator.Element.TYPE;
import static org.infinispan.tools.jdbc.migrator.StoreType.MIXED;

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
import org.infinispan.marshall.core.GlobalMarshaller;
import org.infinispan.persistence.jdbc.DatabaseType;
import org.infinispan.test.data.Person;
import org.infinispan.tools.jdbc.migrator.marshaller.LegacyVersionAwareMarshaller;
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

      MigratorConfiguration config = new MigratorConfiguration(true, properties);
      StreamingMarshaller marshaller = config.getMarshaller();
      assert marshaller != null;
      assert marshaller instanceof GenericJBossMarshaller;
   }

   public void testLegacyMarshallerAndExternalizersLoaded() throws Exception {
      String externalizers = "1:" + PersonExternalizer.class.getName();
      Properties properties = createBaseProperties();
      properties.put(propKey(SOURCE, MARSHALLER, TYPE), MarshallerType.LEGACY.toString());
      properties.put(propKey(SOURCE, MARSHALLER, EXTERNALIZERS), externalizers);

      MigratorConfiguration config = new MigratorConfiguration(true, properties);
      StreamingMarshaller marshaller = config.getMarshaller();
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

      MigratorConfiguration config = new MigratorConfiguration(true, properties);
      StreamingMarshaller marshaller = config.getMarshaller();
      assert marshaller != null;
      assert marshaller instanceof GlobalMarshaller;
      byte[] bytes = marshaller.objectToByteBuffer(new Person(Person.class.getName()));
      Person person = (Person) marshaller.objectFromByteBuffer(bytes);
      assert person != null;
      assert person.getName().equals(Person.class.getName());
      assert externalizerReadCount.get() == 1;
      assert externalizerWriteCount.get() == 1;
   }

   private Properties createBaseProperties() {
      Properties properties = new Properties();
      properties.put(propKey(SOURCE, CACHE_NAME), DEFAULT_CACHE_NAME);
      properties.put(propKey(SOURCE, TYPE), MIXED.toString());
      properties.put(propKey(SOURCE, DIALECT), DatabaseType.H2.toString());
      properties.put(propKey(SOURCE, CONNECTION_POOL, CONNECTION_URL), "jdbc:postgresql:postgres");
      properties.put(propKey(SOURCE, CONNECTION_POOL, DRIVER_CLASS), "org.postgresql.Driver");
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
      public Person readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         externalizerReadCount.incrementAndGet();
         Person person = new Person();
         person.setName(MarshallUtil.unmarshallString(input));
         return person;
      }
   }
}
