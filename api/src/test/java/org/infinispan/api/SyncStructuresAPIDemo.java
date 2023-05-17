package org.infinispan.api;

import org.infinispan.api.sync.SyncContainer;
import org.infinispan.api.sync.SyncList;

/**
 * @author Katia Aresti
 * @since 15.0
 **/
public class SyncStructuresAPIDemo {
   public void demo() {
      try (SyncContainer infinispan = Infinispan.create("file:///path/to/infinispan.xml").sync()) {

         // A default cache multimap is the container we provide
         SyncList<String> list = infinispan.sync().list("listName");

         // set
         list.offerFirst("value");
         // ...

         // additional caches that will contain structures that users may configure
         SyncList<String> list2 = infinispan.sync().select("name").list("listName");

         list2.offerFirst("value");
      }
   }
}
