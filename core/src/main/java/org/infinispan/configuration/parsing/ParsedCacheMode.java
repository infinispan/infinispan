/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */
package org.infinispan.configuration.parsing;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

enum ParsedCacheMode {

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
   
   boolean matches(String candidate) {
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
