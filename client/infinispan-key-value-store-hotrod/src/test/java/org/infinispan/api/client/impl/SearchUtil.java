package org.infinispan.api.client.impl;

import static org.infinispan.functional.FunctionalTestUtils.await;

import org.infinispan.api.reactive.KeyValueStore;
import org.infinispan.commons.util.Util;

public final class SearchUtil {
   public static final String PEOPLE = "people";

   public static final Person OIHANA = new Person("Oihana", "Bilbao", 1984, "Barakaldo");
   public static final Person DANIELA = new Person("Daniela", "Aketxa", 1986, "Donosti");
   public static final Person UNAI = new Person("Unai", "Bilbao", 1988, "Gazteiz");
   public static final Person ELAIA = new Person("Elaia", "Aresti", 1990, "Paris");
   public static final Person MIREN = new Person("Miren", "Bilbao", 1993, "Barakaldo");
   public static final Person EDOIA = new Person("Edoia", "Bilbao", 1990, "Leioa");

   private SearchUtil() {

   }

   static {
      OIHANA.setAddress(new Address("12", "rue des marguettes", "75011", "Paris", "France"));
      DANIELA.setAddress(new Address("187", "rue de charonne", "75011", "Paris", "France"));
      UNAI.setAddress(new Address("16", "rue de la py", "75019", "Paris", "France"));
      ELAIA.setAddress(new Address("26", "rue des marguettes", "75018", "Paris", "France"));
      EDOIA.setAddress(new Address("14", "rue des marguettes", "75011", "Paris", "France"));
      MIREN.setAddress(new Address("14", "rue des marguettes", "75011", "Paris", "France"));
   }

   public static final void populate(KeyValueStore<String, Person> store) {
      await(store.save(OIHANA.id, OIHANA));
      await(store.save(DANIELA.id, DANIELA));
      await(store.save(UNAI.id, UNAI));
      await(store.save(ELAIA.id, ELAIA));
      await(store.save(EDOIA.id, EDOIA));
      await(store.save(MIREN.id, MIREN));
   }

   public static String id() {
      return Util.threadLocalRandomUUID().toString();
   }
}
