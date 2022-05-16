package org.infinispan.it.endpoints;

import java.io.Serializable;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Index;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.Store;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

/**
 * @since 9.2
 */
@Indexed
public class CryptoCurrency implements Serializable {

   @ProtoField(number = 1)
   @Field(analyze = Analyze.NO,index = Index.YES, store = Store.NO)
   String description;

   @ProtoField(number = 2)
   @Field(index = Index.YES, store = Store.NO)
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
