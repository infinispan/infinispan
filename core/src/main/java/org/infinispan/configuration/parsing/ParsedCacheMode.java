package org.infinispan.configuration.parsing;

import static org.infinispan.util.logging.Log.CONFIG;

import java.util.ArrayList;
import java.util.List;

public enum ParsedCacheMode {

   LOCAL("l", "local"),

   REPL("r", "repl", "replication"),

   DIST("d", "dist", "distribution"),

   INVALIDATION("i", "invl", "invalidation");

   private final List<String> synonyms;

   ParsedCacheMode(String... synonyms) {
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
         CONFIG.randomCacheModeSynonymsDeprecated(candidate, name(), synonyms);
         return true;
      }
      return false;
   }

}
