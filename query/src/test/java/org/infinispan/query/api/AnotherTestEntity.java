package org.infinispan.query.api;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;

@Indexed(index="indexB")
public class AnotherTestEntity {

   private final String value;

   public AnotherTestEntity(String value) {
      this.value = value;
   }

   @Field(analyze = Analyze.NO, name="name")
   public String getValue() {
      return value;
   }

}
