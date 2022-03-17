package org.infinispan.test.integration.data;

import org.hibernate.search.mapper.pojo.mapping.definition.annotation.FullTextField;
import org.hibernate.search.mapper.pojo.mapping.definition.annotation.Indexed;
import org.infinispan.protostream.annotations.ProtoField;

@Indexed
public class PersonHibernate {

   @ProtoField(1)
   @FullTextField
   public String name;

   @ProtoField(2)
   public Integer id;

   public PersonHibernate() {
   }

   public PersonHibernate(String name) {
      this.name = name;
   }

   public PersonHibernate(String name, Integer id) {
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
