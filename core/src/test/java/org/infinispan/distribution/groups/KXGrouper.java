package org.infinispan.distribution.groups;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.infinispan.distribution.group.Grouper;

/**
 * A simple grouper which groups String based keys using a pattern for kX keys
 *
 * @author Pete Muir
 *
 */
public class KXGrouper implements Grouper<String> {

   private static final Pattern kPattern = Pattern.compile("(^k)(\\d)$");

   @Override
   public Object computeGroup(String key, Object group) {
      Matcher matcher = kPattern.matcher(key);
      if (matcher.matches()) {
         return Integer.parseInt(matcher.group(2)) % 2 + "";
      } else
         return null;
   }

   @Override
   public Class<String> getKeyType() {
      return String.class;
   }


}
