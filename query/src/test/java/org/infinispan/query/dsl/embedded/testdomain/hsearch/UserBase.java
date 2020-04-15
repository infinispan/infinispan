package org.infinispan.query.dsl.embedded.testdomain.hsearch;

import java.io.Serializable;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.SortableField;
import org.hibernate.search.annotations.Store;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.query.dsl.embedded.testdomain.User;

/**
 * Parent class for UserHS to demonstrate inheritance of indexed attributes.
 */
public abstract class UserBase implements User, Serializable {

   protected String name;

   @Override
   @Field(store = Store.YES, analyze = Analyze.NO)
   @SortableField
   @ProtoField(number = 1)
   public String getName() {
      return name;
   }

   @Override
   public void setName(String name) {
      this.name = name;
   }
}
