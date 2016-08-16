package org.infinispan.query.affinity;

import java.io.Serializable;

import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;

@Indexed(index = "entity")
@SuppressWarnings("unused")
public class Entity implements Serializable {

   @Field
   private final int val;

   public int getVal() {
      return val;
   }

   Entity(int val) {
      this.val = val;
   }
}
