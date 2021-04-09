package org.infinispan.persistence.remote.upgrade;

import java.util.ArrayList;
import java.util.List;

/**
 * @author gustavonalle
 * @since 8.2
 */
public class HotRodMigratorHelper {
   static final int DEFAULT_READ_BATCH_SIZE = 10000;

   static List<Integer> range(int end) {
      List<Integer> integers = new ArrayList<>();
      for (int i = 0; i < end; i++) {
         integers.add(i);
      }
      return integers;
   }

   static <T> List<List<T>> split(List<T> list, final int parts) {
      List<List<T>> subLists = new ArrayList<>(parts);
      for (int i = 0; i < parts; i++) {
         subLists.add(new ArrayList<T>());
      }
      for (int i = 0; i < list.size(); i++) {
         subLists.get(i % parts).add(list.get(i));
      }
      return subLists;
   }
}
