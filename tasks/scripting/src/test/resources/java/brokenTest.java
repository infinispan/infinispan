package script;
// mode=local,language=java

import org.infinispan.commons.CacheException;

public class Script implements java.util.function.Supplier<String> {
   public String get() {
      throw new CacheException("Failed on purpose");
   }
}
