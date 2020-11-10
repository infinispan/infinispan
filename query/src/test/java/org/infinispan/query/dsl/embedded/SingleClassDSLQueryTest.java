package org.infinispan.query.dsl.embedded;

import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.testng.Assert.assertEquals;

import java.util.List;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.Search;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.test.QueryTestSCI;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Test entities defined as inner classes and inheritance of fields using indexed query. Just a simple field equals is
 * tested. The purpose of this test is just to check class and property lookup correctness.
 *
 * @author gustavonalle
 * @author Tristan Tarrant
 * @author anistor@redhat.com
 * @since 8.0
 */
@Test(groups = "functional", testName = "query.dsl.embedded.SingleClassDSLQueryTest")
public class SingleClassDSLQueryTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      configureCache(builder);
      return TestCacheManagerFactory.createCacheManager(QueryTestSCI.INSTANCE, builder);
   }

   protected void configureCache(ConfigurationBuilder builder) {
      builder.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity(Person.class);
   }

   @BeforeClass(alwaysRun = true)
   protected void populateCache() {
      cache.put("person1", new Person("William", "Shakespeare", 50, "ZZ3141592", "M"));
   }

   @Override
   protected void clearContent() {
      // Don't clear, this is destroying the index
   }

   /**
    * Test querying for entities defined as inner classes.
    */
   public void testQueryInnerClass() {
      QueryFactory queryFactory = Search.getQueryFactory(cache);
      Query<Person> query = queryFactory.create("FROM " + Person.class.getName());

      List<Person> matches = query.execute().list();
      assertEquals(1, matches.size());
   }

   /**
    * Test querying for a field - direct access to field.
    */
   public void testField() {
      QueryFactory queryFactory = Search.getQueryFactory(cache);
      Query<Person> query = queryFactory.create("FROM " + Person.class.getName() + " WHERE driverLicenseId = 'ZZ3141592'");

      List<Person> matches = query.execute().list();
      assertEquals(1, matches.size());
   }

   /**
    * Test querying for an inherited indexed field - direct inherited field access.
    */
   public void testInheritedField() {
      QueryFactory queryFactory = Search.getQueryFactory(cache);
      Query<Person> query = queryFactory.create("FROM " + Person.class.getName() + " WHERE age <= 52");

      List<Person> matches = query.execute().list();
      assertEquals(1, matches.size());
   }

   /**
    * Test querying for an inherited indexed field - interface method with inherited implementation.
    */
   public void testInheritedField2() {
      QueryFactory queryFactory = Search.getQueryFactory(cache);
      Query<Person> query = queryFactory.create("FROM " + Person.class.getName() + " WHERE name <= 'William'");

      List<Person> matches = query.execute().list();
      assertEquals(1, matches.size());
   }

   /**
    * Test querying for an inherited indexed field - interface method implemented in class.
    */
   public void testInheritedField3() {
      QueryFactory queryFactory = Search.getQueryFactory(cache);
      Query<Person> query = queryFactory.create("FROM " + Person.class.getName() + " WHERE gender = 'M'");

      List<Person> matches = query.execute().list();
      assertEquals(1, matches.size());
   }

   /**
    * Test querying for an inherited indexed field - method inherited from superclass.
    */
   public void testInheritedField4() {
      QueryFactory queryFactory = Search.getQueryFactory(cache);
      Query<Person> query = queryFactory.create("FROM " + Person.class.getName() + " WHERE surname = 'Shakespeare'");

      List<Person> matches = query.execute().list();
      assertEquals(1, matches.size());
   }

   interface PersonInterface {

      String getName();

      String getGender();
   }

   public static abstract class PersonBase implements PersonInterface {

      String name;

      String surname;

      int age;

      PersonBase(String name, String surname, int age) {
         this.name = name;
         this.surname = surname;
         this.age = age;
      }

      @Field(analyze = Analyze.NO)
      @Override
      public String getName() {
         return name;
      }

      @Field(analyze = Analyze.NO)
      public String getSurname() {
         return surname;
      }

      @Field(analyze = Analyze.NO)
      public int getAge() {
         return age;
      }
   }

   @Indexed
   public static class Person extends PersonBase {

      String driverLicenseId;

      String gender;

      public Person(String name, String surname, int age, String driverLicenseId, String gender) {
         super(name, surname, age);
         this.driverLicenseId = driverLicenseId;
         this.gender = gender;
      }

      @Field(analyze = Analyze.NO)
      public String getDriverLicenseId() {
         return driverLicenseId;
      }

      @Field(analyze = Analyze.NO)
      @Override
      public String getGender() {
         return gender;
      }
   }
}
