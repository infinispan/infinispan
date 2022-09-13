package org.infinispan.persistence.sql;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.stream.Stream;

import org.infinispan.Cache;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.test.ExceptionRunnable;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.manager.EmbeddedCacheManagerStartupException;
import org.infinispan.persistence.jdbc.common.UnitTestDatabaseManager;
import org.infinispan.persistence.jdbc.common.configuration.ConnectionFactoryConfiguration;
import org.infinispan.persistence.jdbc.common.configuration.ConnectionFactoryConfigurationBuilder;
import org.infinispan.persistence.jdbc.common.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.sql.configuration.QueriesJdbcConfigurationBuilder;
import org.infinispan.persistence.sql.configuration.QueriesJdbcStoreConfigurationBuilder;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.data.Address;
import org.infinispan.test.data.Person;
import org.infinispan.test.data.Sex;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "persistence.sql.QueriesJdbcJoinTest")
public class QueriesJdbcJoinTest extends AbstractInfinispanTest {
   private static final String TABLE1_NAME = "Person";
   private static final String TABLE2_NAME = "Address";

   private ConnectionFactory FACTORY;

   @AfterMethod(alwaysRun = true)
   public void afterClass() {
      if (FACTORY != null) {
         FACTORY.stop();
      }
   }

   enum TestType {
      TOO_MANY_COLUMNS {
         @Override
         void runTest(ExceptionRunnable runnable) {
            Exceptions.expectException(".*Additional value columns.*found that were not part of the schema.*", runnable,
                  EmbeddedCacheManagerStartupException.class, CacheConfigurationException.class,
                  CompletionException.class, CacheConfigurationException.class);
         }

         @Override
         void modifyConfiguration(QueriesJdbcStoreConfigurationBuilder builder, boolean idJoin) {
            super.modifyConfiguration(builder, idJoin);
            QueriesJdbcConfigurationBuilder queryBuilder = builder.queries();
            if (idJoin) {
               queryBuilder
                     // Note these return * which will include the joined columns as well
                     .select("SELECT * FROM " + TABLE1_NAME + " t1 JOIN " + TABLE2_NAME + " t2 ON t1.address = t2.id WHERE t1.name = :name")
                     .selectAll("SELECT * FROM " + TABLE1_NAME + " t1 JOIN " + TABLE2_NAME + " t2 ON t1.address = t2.id");

            } else {
               queryBuilder
                     // Note these return * which will include the joined columns as well
                     .select("SELECT * FROM " + TABLE1_NAME + " t1 JOIN " + TABLE2_NAME + " t2 WHERE t1.name = :name AND t2.name = :name")
                     .selectAll("SELECT * FROM " + TABLE1_NAME + " t1 JOIN " + TABLE2_NAME + " t2 WHERE t1.name = t2.name");
            }
         }
      },
      NOT_EMBEDDED_KEY {
         @Override
         void runTest(ExceptionRunnable runnable) {
            Exceptions.expectException(".*was found in the value schema .* but embedded key was not true", runnable,
                  EmbeddedCacheManagerStartupException.class, CacheConfigurationException.class,
                  CompletionException.class, CacheConfigurationException.class);
         }

         @Override
         void modifyConfiguration(QueriesJdbcStoreConfigurationBuilder builder, boolean idJoin) {
            super.modifyConfiguration(builder, idJoin);
            builder.schema().embeddedKey(false);
         }
      },
      PASS;

      void runTest(ExceptionRunnable runnable) throws Exception {
         runnable.run();
      }

      void modifyConfiguration(QueriesJdbcStoreConfigurationBuilder builder, boolean idJoin) {
         QueriesJdbcConfigurationBuilder queryBuilder = builder.queries();
         queryBuilder.size("SELECT COUNT(*) FROM " + TABLE1_NAME);
         builder.schema().embeddedKey(true);
         if (idJoin) {
            builder.queries()
                  .select("SELECT t1.name, t1.picture, t1.sex, t1.birthdate, t1.accepted_tos, t2.street, t2.city, t2.zip FROM " + TABLE1_NAME + " t1 JOIN " + TABLE2_NAME + " t2 ON t1.address = t2.id WHERE t1.name = :name")
                  .selectAll("SELECT t1.name, t1.picture, t1.sex, t1.birthdate, t1.accepted_tos, t2.street, t2.city, t2.zip FROM " + TABLE1_NAME + " t1 JOIN " + TABLE2_NAME + " t2 ON t1.address = t2.id");
         } else {
            builder.queries()
                  .select("SELECT t1.name, t1.picture, t1.sex, t1.birthdate, t1.accepted_tos, t2.street, t2.city, t2.zip FROM " + TABLE1_NAME + " t1 JOIN " + TABLE2_NAME + " t2 WHERE t1.name = :name AND t2.name = :name")
                  .selectAll("SELECT t1.name, t1.picture, t1.sex, t1.birthdate, t1.accepted_tos, t2.street, t2.city, t2.zip FROM " + TABLE1_NAME + " t1 JOIN " + TABLE2_NAME + " t2 WHERE t1.name = t2.name");
         }
      }
   }

   protected EmbeddedCacheManager createCacheManager(TestType type, boolean idJoin) {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.encoding().mediaType(MediaType.APPLICATION_PROTOSTREAM_TYPE);
      QueriesJdbcStoreConfigurationBuilder queriesBuilder = builder.persistence()
            .addStore(QueriesJdbcStoreConfigurationBuilder.class)
            .ignoreModifications(true);
      queriesBuilder.keyColumns("name");
      queriesBuilder.schema()
            .messageName("Person")
            .packageName("org.infinispan.test.core");

      UnitTestDatabaseManager.configureUniqueConnectionFactory(queriesBuilder);

      createTables(queriesBuilder.getConnectionFactory(), idJoin);

      type.modifyConfiguration(queriesBuilder, idJoin);

      return TestCacheManagerFactory.createCacheManager(TestDataSCI.INSTANCE, builder);
   }

   private void createTables(ConnectionFactoryConfigurationBuilder<ConnectionFactoryConfiguration> builder, boolean idJoin) {
      ConnectionFactoryConfiguration config = builder.create();
      FACTORY = ConnectionFactory.getConnectionFactory(config.connectionFactoryClass());
      FACTORY.start(config, getClass().getClassLoader());
      Connection connection = null;
      try {
         connection = FACTORY.getConnection();
         try (Statement stmt = connection.createStatement()) {
            String tableCreation = "CREATE TABLE " + TABLE1_NAME + " (" +
                  "name VARCHAR(255) NOT NULL, " +
                  (idJoin ? "address INT, " : "") +
                  "picture VARBINARY(255), " +
                  "sex VARCHAR(255), " +
                  "birthdate TIMESTAMP, " +
                  "accepted_tos boolean, " +
                  "notused VARCHAR(255), " +
                  "PRIMARY KEY (NAME))";
            stmt.execute(tableCreation);
            tableCreation = "create TABLE " + TABLE2_NAME + " (" +
                  (idJoin ? "id INT NOT NULL, " : "name VARCHAR(255) NOT NULL, ") +
                  "street VARCHAR(255), " +
                  "city VARCHAR(255), " +
                  "zip INT, " +
                  "PRIMARY KEY (" + (idJoin ? "id" : "name") + "))";
            stmt.execute(tableCreation);
         }
      } catch (SQLException t) {
         throw new AssertionError(t);
      } finally {
         FACTORY.releaseConnection(connection);
      }
   }

   public void testUpsertMultipleValues() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.encoding().mediaType(MediaType.APPLICATION_PROTOSTREAM_TYPE);
      QueriesJdbcStoreConfigurationBuilder queriesBuilder = builder.persistence()
            .addStore(QueriesJdbcStoreConfigurationBuilder.class);
      queriesBuilder.keyColumns("name");
      queriesBuilder.schema()
            .messageName("Person")
            .packageName("org.infinispan.test.core");

      UnitTestDatabaseManager.configureUniqueConnectionFactory(queriesBuilder);

      createTables(queriesBuilder.getConnectionFactory(), false);

      TestType.PASS.modifyConfiguration(queriesBuilder, false);

      queriesBuilder.queries()
            .delete("DELETE FROM " + TABLE1_NAME + " t1 WHERE t1.name = :name; DELETE FROM " + TABLE2_NAME + " t2 where t2.name = :name")
            .deleteAll("DELETE FROM " + TABLE1_NAME + "; DELETE FROM " + TABLE2_NAME)
            .upsert(insertTable1Statement(false, true) +
                  "; " + insertTable2Statement(false, true));

      EmbeddedCacheManager embeddedCacheManager = TestCacheManagerFactory.createCacheManager(TestDataSCI.INSTANCE, builder);
      Cache<String, Person> cache = embeddedCacheManager.getCache();
      String name = "Mircea Markus";
      Person person = samplePerson(name);
      cache.put(name, person);

      assertEquals(person, cache.get(name));

      cache.remove(name);

      assertNull(cache.get(name));
   }

   @DataProvider(name = "testTypes")
   public static Object[][] testTypes() {
      return Stream.of(TestType.values())
            .flatMap(t -> Stream.of(new Object[]{t, true}, new Object[]{t, false}))
            .toArray(Object[][]::new);
   }

   @Test(dataProvider = "testTypes")
   public void testIdJoinTypes(TestType type, boolean idJoin) throws Exception {
      type.runTest(() -> {
         EmbeddedCacheManager cacheManager = createCacheManager(type, idJoin);
         Cache<String, Person> cache = cacheManager.getCache();
         Connection connection = FACTORY.getConnection();
         try {
            String name = "Manik Surtani";
            Person person = samplePerson(name);
            insertData(connection, Collections.singleton(person), idJoin);

            assertEquals(person, cache.get(name));
         } finally {
            FACTORY.releaseConnection(connection);
         }
      });
   }

   private Person samplePerson(String name) {
      Address address = new Address();
      address.setCity("London");
      address.setStreet("Cool Street");
      address.setZip(1321);
      Person person = new Person(name, address);
      person.setPicture(new byte[]{0x1, 0x12});
      person.setSex(Sex.MALE);
      person.setBirthDate(new java.util.Date(1629495308));
      person.setAcceptedToS(true);
      return person;
   }

   private String insertTable1Statement(boolean idJoin, boolean namedParams) {
      return "INSERT INTO " + TABLE1_NAME +
            " (name, " + (idJoin ? "address, " : "") + " picture, sex, birthdate, accepted_tos) " +
            (namedParams ? "VALUES (:name" + (idJoin ? ", :address" : "") + ", :picture, :sex, :birthdate, :accepted_tos)" :
                  "VALUES (?, ?, ?, ?, ?" + (idJoin ? ", ?)" : ")"));
   }

   private String insertTable2Statement(boolean idJoin, boolean namedParams) {
      return "INSERT INTO " + TABLE2_NAME +
            "(" + (idJoin ? "id" : "name") + ", street, city, zip) " +
            (namedParams ? "VALUES (" + (idJoin ? ":id" : ":name") + ", :street, :city, :zip)" :
                  "VALUES (?, ?, ?, ?)");
   }

   private void insertData(Connection connection, Set<Person> peopleToCreate, boolean idJoin) throws SQLException {
      String insertStatement = insertTable1Statement(idJoin, false);
      int addressCount = 0;
      Map<Address, Integer> addressIntegerMap = idJoin ? new HashMap<>() : null;
      try (PreparedStatement ps = connection.prepareStatement(insertStatement)) {
         for (Person person : peopleToCreate) {
            int offset = 1;
            ps.setString(offset++, person.getName());
            if (addressIntegerMap != null) {
               Address address = person.getAddress();

               Integer addressNumber = addressIntegerMap.get(address);
               if (addressNumber == null) {
                  addressNumber = addressCount++;
                  addressIntegerMap.put(address, addressNumber);
               }
               ps.setInt(offset++, addressNumber);
            }
            ps.setBytes(offset++, person.getPicture());
            ps.setString(offset++, person.getSex().toString());
            ps.setTimestamp(offset++, new Timestamp(person.getBirthDate().getTime()));
            ps.setBoolean(offset, person.isAcceptedToS());

            ps.addBatch();
         }
         ps.executeBatch();
      }

      insertStatement = insertTable2Statement(idJoin, false);
      try (PreparedStatement ps = connection.prepareStatement(insertStatement)) {
         for (Person person : peopleToCreate) {
            Address address = person.getAddress();
            if (addressIntegerMap != null) {
               Integer id = addressIntegerMap.get(address);
               assert id != null;
               ps.setInt(1, id);
            } else {
               ps.setString(1, person.getName());
            }
            ps.setString(2, address.getStreet());
            ps.setString(3, address.getCity());
            ps.setInt(4, address.getZip());

            ps.addBatch();
         }
         ps.executeBatch();
      }
   }
}
