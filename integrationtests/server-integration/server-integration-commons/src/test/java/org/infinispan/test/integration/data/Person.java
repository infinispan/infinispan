package org.infinispan.test.integration.data;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.SortableField;
import org.hibernate.search.annotations.Store;
import org.infinispan.protostream.annotations.ProtoField;

@Indexed
public class Person {

   @ProtoField(1)
   @Field(store = Store.YES, analyze = Analyze.NO)
   @SortableField
   public String name;

   @ProtoField(2)
   public Integer id;

   public Person() {
   }

   public Person(String name) {
      this.name = name;
   }

   public Person(String name, Integer id) {
      this.name = name;
      this.id = id;
   }

   public String getName() {
      return name;
   }

   public Integer getId() {
      return id;
   }
}
