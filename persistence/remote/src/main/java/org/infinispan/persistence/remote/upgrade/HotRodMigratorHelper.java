package org.infinispan.persistence.remote.upgrade;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.impl.protocol.VersionUtils;
import org.infinispan.persistence.remote.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * @author gustavonalle
 * @since 8.2
 */
public class HotRodMigratorHelper {

   static final String MIGRATION_MANAGER_HOT_ROD_KNOWN_KEYS = "___MigrationManager_HotRod_KnownKeys___";
   static final String ITERATOR_MINIMUM_VERSION = "2.5";
   static final int DEFAULT_READ_BATCH_SIZE = 10000;

   private static final Log log = LogFactory.getLog(HotRodMigratorHelper.class, Log.class);

   static boolean supportsIteration(String protocolVersion) {
      return protocolVersion == null || VersionUtils.isVersionGreaterOrEquals(protocolVersion, ITERATOR_MINIMUM_VERSION);
   }

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

   static void awaitTermination(ExecutorService executorService) {
      try {
         executorService.shutdown();
         executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
      }
   }

}
