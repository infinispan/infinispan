package org.infinispan.test.integration.as.embedded;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT_TYPE;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.List;

import org.infinispan.Cache;
import org.infinispan.commons.util.Version;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.test.integration.data.Person;
import org.infinispan.test.integration.data.PersonHibernate;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.Asset;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.jboss.shrinkwrap.descriptor.api.Descriptors;
import org.jboss.shrinkwrap.descriptor.api.spec.se.manifest.ManifestDescriptor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(Arquillian.class)
public class QueryIT {

   private EmbeddedCacheManager cm;

   @After
   public void cleanUp() {
      if (cm != null)
         cm.stop();
   }

   @Before
   public void start() {
      cm = new DefaultCacheManager();
      cm.createCache("myCache", new ConfigurationBuilder()
            .encoding().mediaType(APPLICATION_OBJECT_TYPE)
            .indexing().enable().storage(LOCAL_HEAP)
            .addIndexedEntity(Person.class)
            .build());
      cm.createCache("myCacheHibernate", new ConfigurationBuilder()
            .indexing().enable().storage(LOCAL_HEAP)
            .addIndexedEntity(PersonHibernate.class)
            .build());
   }

   @Deployment
   public static Archive<?> deployment() {
      return ShrinkWrap
            .create(WebArchive.class, "query.war")
            .addClass(QueryIT.class)
            .addClass(Person.class)
            .addClass(PersonHibernate.class)
            .add(manifest(), "META-INF/MANIFEST.MF");
   }

   private static Asset manifest() {
      String manifest = Descriptors.create(ManifestDescriptor.class).attribute("Dependencies", "org.infinispan:" + Version.getModuleSlot() + " services").exportAsString();
      return new StringAsset(manifest);
   }

   @Test
   public void testLuceneQuery() {
      Cache<Integer, Person> cache = cm.getCache("myCache");
      cache.put(1, new Person("foo", 1));
      cache.put(2, new Person("bar", 2));

      QueryFactory queryFactory = org.infinispan.query.Search.getQueryFactory(cache);
      Query<Person> query = queryFactory.create(String.format("FROM %s WHERE name LIKE '%%fo%%'", Person.class.getName()));
      List<Person> list = query.execute().list();

      assertNotNull(list);
      assertEquals(1, list.size());
      assertEquals("foo", list.get(0).getName());
   }

   @Test
   public void testLuceneQueryHibernate() {
      Cache<Integer, PersonHibernate> cache = cm.getCache("myCacheHibernate");
      cache.put(1, new PersonHibernate("foo", 1));
      cache.put(2, new PersonHibernate("bar", 2));

      QueryFactory queryFactory = org.infinispan.query.Search.getQueryFactory(cache);
      Query<PersonHibernate> query = queryFactory.create(String.format("FROM %s p WHERE p.name:'foo'", PersonHibernate.class.getName()));
      List<PersonHibernate> list = query.execute().list();

      assertNotNull(list);
      assertEquals(1, list.size());
      assertEquals("foo", list.get(0).getName());
   }
}
