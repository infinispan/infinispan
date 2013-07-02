package org.infinispan.query.test;

import org.hibernate.search.annotations.DocumentId;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;

/**
 * @author Navin Surtani
 */
@Indexed
public class BrokenDocumentId {
   @DocumentId
   @Field
   String name;

   @Field
   int age;

   public void setBoth(String name, int age) {
      this.name = name;
      this.age = age;
   }
}
