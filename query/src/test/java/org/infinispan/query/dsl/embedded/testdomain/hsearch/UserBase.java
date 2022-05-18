package org.infinispan.query.dsl.embedded.testdomain.hsearch;

import java.io.Serializable;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.query.dsl.embedded.testdomain.User;

/**
 * Parent class for UserHS to demonstrate inheritance of indexed attributes.
 */
public abstract class UserBase implements User, Serializable {

   protected String name;

   @Override
   @Basic(projectable = true, sortable = true)
   @ProtoField(number = 1)
   public String getName() {
      return name;
   }

   @Override
   public void setName(String name) {
      this.name = name;
   }
}
