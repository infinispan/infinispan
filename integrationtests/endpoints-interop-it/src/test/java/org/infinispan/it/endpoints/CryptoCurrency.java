package org.infinispan.it.endpoints;

import java.io.Serializable;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.infinispan.protostream.annotations.ProtoDoc;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoMessage;

/**
 * @since 9.2
 */
@Indexed
@ProtoMessage(name = "CryptoCurrency")
@ProtoDoc("@Indexed")
public class CryptoCurrency implements Serializable {

   @Field(analyze = Analyze.NO)
   @ProtoField(number = 1)
   @ProtoDoc("@Field(index = Index.YES, store = Store.NO)")
   public String description;

   @Field
   @ProtoField(number = 2)
   @ProtoDoc("@Field(index = Index.YES, store = Store.NO)")
   public Integer rank;

   public CryptoCurrency() {
   }

   public CryptoCurrency(String description, Integer rank) {
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
