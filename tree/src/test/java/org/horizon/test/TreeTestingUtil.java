package org.horizon.test;

import org.horizon.tree.TreeCache;

import java.util.Collection;

public class TreeTestingUtil {
   public static void killTreeCaches(Collection treeCaches) {
      if (treeCaches != null) killTreeCaches((TreeCache[]) treeCaches.toArray(new TreeCache[]{}));
   }

   public static void killTreeCaches(TreeCache... treeCaches) {
      for (TreeCache tc : treeCaches) {
         if (tc != null) TestingUtil.killCaches(tc.getCache());
      }
   }
}
