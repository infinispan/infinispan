package org.infinispan.query.api;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

@Indexed(index = "indexB")
public class AnotherTestEntity {

   private final String value;

   @ProtoFactory
   AnotherTestEntity(String value) {
      this.value = value;
   }

   @Field(analyze = Analyze.NO, name="name")
   @ProtoField(1)
   public String getValue() {
      return value;
   }
}
