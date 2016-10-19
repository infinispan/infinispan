package org.infinispan.query.affinity;

import java.io.Serializable;

import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.Store;
import org.infinispan.marshall.core.ExternalPojo;

@Indexed(index = "entity")
@SuppressWarnings("unused")
public class Entity implements Serializable, ExternalPojo {

   @Field(store = Store.YES)
   private final int val;

   public int getVal() {
      return val;
   }

   Entity(int val) {
      this.val = val;
   }
}
