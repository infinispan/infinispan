package org.infinispan.commons.configuration;

import java.util.Collection;
import java.util.List;

/**
 * @deprecated since 12.0. Will be removed in 14.0. Use {@link ClassAllowList}.
 **/
@Deprecated
public final class ClassWhiteList extends ClassAllowList {
   public ClassWhiteList() {
   }

   public ClassWhiteList(List<String> regexps) {
      super(regexps);
   }

   public ClassWhiteList(Collection<String> classes, List<String> regexps, ClassLoader classLoader) {
      super(classes, regexps, classLoader);
   }
}
