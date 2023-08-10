package org.infinispan.it.endpoints;

import java.io.Serializable;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

/**
 * @since 9.2
 */
@Indexed
public class CryptoCurrency implements Serializable {

   @ProtoField(number = 1)
   @Basic
   String description;

   @ProtoField(number = 2)
   @Basic
   Integer rank;

   CryptoCurrency() {}

   @ProtoFactory
   CryptoCurrency(String description, Integer rank) {
      this.description = description;
      this.rank = rank;
   }

   public String getDescription() {
      return description;
   }

   public Integer getRank() {
      return rank;
   }
}
