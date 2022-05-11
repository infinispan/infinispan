package org.infinispan.query.model;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.api.annotations.indexing.Keyword;
import org.infinispan.api.annotations.indexing.Text;

@Indexed
public class Color {

   @Basic
   private final String name;

   @Basic(name = "desc1")
   @Keyword(name = "desc2")
   @Text(name = "desc3")
   private final String description;

   public Color(String name, String description) {
      this.name = name;
      this.description = description;
   }

   public String getName() {
      return name;
   }

   public String getDescription() {
      return description;
   }
}
