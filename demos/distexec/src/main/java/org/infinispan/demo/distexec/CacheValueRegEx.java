package org.infinispan.demo.distexec;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.infinispan.Cache;
import org.infinispan.distexec.DistributedCallable;

@SuppressWarnings("serial")
public class CacheValueRegEx<K, V, T> implements DistributedCallable<K, String, List<K>>, Serializable {

   Cache<K, String> cache;
   Set<K> keys;

   public String pattern = "The Project Gutenberg EBook of The Complete Works of William Shakespeare";

   @Override
   public void setEnvironment(Cache<K, String> cache, Set<K> keys) {
      this.cache = cache;
      this.keys = keys;
   }

   @Override
   public List<K> call() throws Exception {
      List<K> result = new ArrayList<K>();
      Pattern regexp = Pattern.compile(pattern);
      Logger logger = Logger.getLogger(getClass().getName());

      logger.info("Searching for regular expression '" + pattern + "'");
      for (Entry<K, String> entry : this.cache.entrySet()) {
         int foundCount = 0;
         String valueString = entry.getValue();

         Matcher matcher = regexp.matcher(valueString);
         while (matcher.find()) {
            foundCount++;
         }
         if (foundCount > 0) {
            logger.finest("Found regular expression '" + pattern + "' " + foundCount + " times in key '"
                  + entry.getKey() + "'");
            result.add(entry.getKey());
         }
      }

      return result;
   }

   public void setPattern(String pattern) {
      this.pattern = pattern;
   }

}
