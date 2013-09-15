package org.infinispan.configuration.parsing;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

public enum ParsedCacheMode {

   LOCAL("l", "local"),
   
   REPL("r", "repl", "replication"),
   
   DIST("d", "dist", "distribution"),
  
   INVALIDATION("i", "invl", "invalidation");
   
   private static final Log log = LogFactory.getLog(ParsedCacheMode.class);
   
   private final List<String> synonyms;
   
   private ParsedCacheMode(String... synonyms) {
      this.synonyms = new ArrayList<String>();
      for (String synonym : synonyms) {
         this.synonyms.add(synonym.toUpperCase());
      }
   }
   
   public boolean matches(String candidate) {
      String c = candidate.toUpperCase();
      for (String synonym : synonyms) {
         if (c.equals(synonym))
            return true;
      }
      if (c.toUpperCase().startsWith(name().toUpperCase().substring(0, 1))) {
         log.randomCacheModeSynonymsDeprecated(candidate, name(), synonyms);
         return true;
      }
      return false;
   }
   
}
