package org.infinispan.distribution.groups;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.distribution.group.Group;
import org.infinispan.distribution.group.GroupManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class contains some utility methods to the grouping advanced interface tests.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public abstract class BaseUtilGroupTest extends MultipleCacheManagersTest {

   protected static final String GROUP = "test-group";
   protected final TestCacheFactory factory;

   protected BaseUtilGroupTest(TestCacheFactory factory) {
      this.factory = factory;
   }

   protected static GroupKey key(int index) {
      return new GroupKey(GROUP, index);
   }

   protected static String value(int index) {
      return "v" + index;
   }

   protected abstract void resetCaches(List<Cache<GroupKey, String>> cacheList);

   protected static boolean isGroupOwner(Cache<?, ?> cache, String groupName) {
      return TestingUtil.extractComponent(cache, GroupManager.class).isOwner(groupName);
   }

   protected static AdvancedCache<GroupKey, String> extractTargetCache(TestCache testCache) {
      if (isGroupOwner(testCache.testCache, GROUP)) {
         return testCache.testCache;
      } else {
         //the command will be forwarded to the primary owner.
         return testCache.primaryOwner.getAdvancedCache();
      }
   }

   protected static void initCache(Cache<GroupKey, String> cache) {
      for (int i = 0; i < 10; ++i) {
         cache.put(key(i), value(i));
         cache.put(new GroupKey("other-group", i), value(i));
      }
   }

   protected static Map<GroupKey, String> createMap(int from, int to) {
      Map<GroupKey, String> map = new HashMap<>();
      for (int i = from; i < to; ++i) {
         map.put(key(i), value(i));
      }
      return map;
   }

   protected final TestCache createTestCacheAndReset(String groupName, List<Cache<GroupKey, String>> cacheList) {
      resetCaches(cacheList);
      return factory.create(groupName, cacheList);
   }

   public static enum TestCacheFactory {
      PRIMARY_OWNER {
         @Override
         public TestCache create(String groupName, List<Cache<GroupKey, String>> cacheList) {
            for (Cache<GroupKey, String> cache : cacheList) {
               GroupManager groupManager = TestingUtil.extractComponent(cache, GroupManager.class);
               if (groupManager.isPrimaryOwner(groupName)) {
                  return new TestCache(cache, cache.getAdvancedCache());
               }
            }
            throw new IllegalStateException("didn't find a cache... should never happen!");
         }
      },
      BACKUP_OWNER {
         @Override
         public TestCache create(String groupName, List<Cache<GroupKey, String>> cacheList) {
            Cache<GroupKey, String> primaryOwner = null;
            AdvancedCache<GroupKey, String> backupOwner = null;
            for (Cache<GroupKey, String> cache : cacheList) {
               GroupManager groupManager = TestingUtil.extractComponent(cache, GroupManager.class);
               if (primaryOwner == null && groupManager.isPrimaryOwner(groupName)) {
                  primaryOwner = cache;
               } else if (backupOwner == null && groupManager.isOwner(groupName)) {
                  backupOwner = cache.getAdvancedCache();
               }
               if (primaryOwner != null && backupOwner != null) {
                  return new TestCache(primaryOwner, backupOwner);
               }
            }
            throw new IllegalStateException("didn't find a cache... should never happen!");
         }
      },
      NON_OWNER {
         @Override
         public TestCache create(String groupName, List<Cache<GroupKey, String>> cacheList) {
            Cache<GroupKey, String> primaryOwner = null;
            AdvancedCache<GroupKey, String> nonOwner = null;
            for (Cache<GroupKey, String> cache : cacheList) {
               GroupManager groupManager = TestingUtil.extractComponent(cache, GroupManager.class);
               if (primaryOwner == null && groupManager.isPrimaryOwner(groupName)) {
                  primaryOwner = cache;
               } else if (nonOwner == null && !groupManager.isOwner(groupName)) {
                  nonOwner = cache.getAdvancedCache();
               }
               if (primaryOwner != null && nonOwner != null) {
                  return new TestCache(primaryOwner, nonOwner);
               }
            }
            throw new IllegalStateException("didn't find a cache... should never happen!");
         }
      };

      public abstract TestCache create(String groupName, List<Cache<GroupKey, String>> cacheList);
   }

   public static class GroupKey implements Serializable {

      private final String group;
      private final int key;

      GroupKey(String group, int key) {
         this.group = group;
         this.key = key;
      }

      @Group
      public String getGroup() {
         return group;
      }

      public int getKey() {
         return key;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         GroupKey groupKey = (GroupKey) o;

         return key == groupKey.key && group.equals(groupKey.group);
      }

      @Override
      public int hashCode() {
         int result = group.hashCode();
         result = 31 * result + key;
         return result;
      }

      @Override
      public String toString() {
         return "GroupKey{" +
               "group='" + group + '\'' +
               ", key=" + key +
               '}';
      }
   }

   public static class TestCache {
      public final Cache<GroupKey, String> primaryOwner;
      public final AdvancedCache<GroupKey, String> testCache;

      public TestCache(Cache<GroupKey, String> primaryOwner, AdvancedCache<GroupKey, String> testCache) {
         this.primaryOwner = primaryOwner;
         this.testCache = testCache;
      }
   }
}
