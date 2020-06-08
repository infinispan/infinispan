package script;
// mode=local,language=java,parameters=[a]

public class Script implements java.util.function.Supplier<String> {
   public static org.infinispan.manager.EmbeddedCacheManager cacheManager;
   public static String a;

   public String get() {
      org.infinispan.Cache<String, String> cache = cacheManager.getCache("script-exec");
      cache.put("a", a);
      return cache.get("a");
   }
}

