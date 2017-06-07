package org.infinispan.query.dsl.embedded;

import static org.testng.Assert.assertEquals;

import java.util.List;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.Search;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
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
      return TestCacheManagerFactory.createCacheManager(builder);
   }

   protected void configureCache(ConfigurationBuilder builder) {
      builder.indexing().index(Index.ALL)
            .addIndexedEntity(Person.class)
            .addProperty("default.directory_provider", "ram")
            .addProperty("lucene_version", "LUCENE_CURRENT");
   }

   @BeforeClass(alwaysRun = true)
   protected void populateCache() throws Exception {
      cache.put("person1", new Person("William", "Shakespeare", 50, "ZZ3141592", "M"));
   }

   @Override
   protected void clearContent() {
      // Don't clear, this is destroying the index
   }

   /**
    * Test querying for entities defined as inner classes.
    */
   public void testQueryInnerClass() throws Exception {
      QueryFactory queryFactory = Search.getQueryFactory(cache);
      Query query = queryFactory.from(Person.class).build();

      List<Person> matches = query.list();
      assertEquals(1, matches.size());
   }

   /**
    * Test querying for a field - direct access to field.
    */
   public void testField() throws Exception {
      QueryFactory queryFactory = Search.getQueryFactory(cache);
      Query query = queryFactory.from(Person.class)
            .having("driverLicenseId").eq("ZZ3141592")
            .build();

      List<Person> matches = query.list();
      assertEquals(1, matches.size());
   }

   /**
    * Test querying for an inherited indexed field - direct inherited field access.
    */
   public void testInheritedField() throws Exception {
      QueryFactory queryFactory = Search.getQueryFactory(cache);
      Query query = queryFactory.from(Person.class)
            .having("age").lte(52)
            .build();

      List<Person> matches = query.list();
      assertEquals(1, matches.size());
   }

   /**
    * Test querying for an inherited indexed field - interface method with inherited implementation.
    */
   public void testInheritedField2() throws Exception {
      QueryFactory queryFactory = Search.getQueryFactory(cache);
      Query query = queryFactory.from(Person.class)
            .having("name").eq("William")
            .build();

      List<Person> matches = query.list();
      assertEquals(1, matches.size());
   }

   /**
    * Test querying for an inherited indexed field - interface method implemented in class.
    */
   public void testInheritedField3() throws Exception {
      QueryFactory queryFactory = Search.getQueryFactory(cache);
      Query query = queryFactory.from(Person.class)
            .having("gender").eq("M")
            .build();

      List<Person> matches = query.list();
      assertEquals(1, matches.size());
   }

   /**
    * Test querying for an inherited indexed field - method inherited from superclass.
    */
   public void testInheritedField4() throws Exception {
      QueryFactory queryFactory = Search.getQueryFactory(cache);
      Query query = queryFactory.from(Person.class)
            .having("surname").eq("Shakespeare")
            .build();

      List<Person> matches = query.list();
      assertEquals(1, matches.size());
   }

   interface PersonInterface {

      String getName();

      String getGender();
   }

   static abstract class PersonBase implements PersonInterface {

      String name;

      String surname;

      @Field(analyze = Analyze.NO)
      int age;

      PersonBase(String name, String surname, int age) {
         this.name = name;
         this.surname = surname;
         this.age = age;
      }

      @Field(analyze = Analyze.NO, indexNullAs = Field.DEFAULT_NULL_TOKEN)
      public String getSurname() {
         return surname;
      }

      @Field(analyze = Analyze.NO)
      @Override
      public String getName() {
         return name;
      }
   }

   @Indexed
   static class Person extends PersonBase {

      @Field(analyze = Analyze.NO)
      String driverLicenseId;

      String gender;

      public Person(String name, String surname, int age, String driverLicenseId, String gender) {
         super(name, surname, age);
         this.driverLicenseId = driverLicenseId;
         this.gender = gender;
      }

      @Field(analyze = Analyze.NO)
      @Override
      public String getGender() {
         return gender;
      }
   }
}
