package org.infinispan.query.affinity;

import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.Store;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

@Indexed(index = "entity")
@SuppressWarnings("unused")
public class Entity {

   @Field(store = Store.YES)
   private final int val;

   @ProtoFactory
   Entity(int val) {
      this.val = val;
   }

   @ProtoField(number = 1, defaultValue = "0")
   public int getVal() {
      return val;
   }
}
