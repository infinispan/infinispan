package org.infinispan.query.test;

import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;

/**
 * @author Navin Surtani (<a href="mailto:nsurtani@redhat.com">nsurtani@redhat.com</a>)
 */
@Indexed
public class BrokenProvided {
   @Field
   public String name;

   @Field
   public int age;

   public void setBoth(String name, int age) {
      this.name = name;
      this.age = age;

   }

}
