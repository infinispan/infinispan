package org.infinispan.query.api;

import java.io.Serializable;

import org.infinispan.marshall.core.ExternalPojo;

/**
 * A test value having a non-standard constructor, no setter and not indexed
 */
public class NotIndexedType implements Serializable, ExternalPojo {

   public NotIndexedType(String name) {
      this.name = name;
   }

   private final String name;

   public String getName() {
      return name;
   }

}
