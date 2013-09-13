package org.infinispan.client.hotrod.marshall;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.Store;
import org.infinispan.protostream.sampledomain.Account;

import java.util.Date;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
@Indexed
public class EmbeddedAccount extends Account {

   @Field(store = Store.YES, analyze = Analyze.NO)
   @Override
   public int getId() {
      return super.getId();
   }

   @Field(store = Store.YES, analyze = Analyze.NO)
   @Override
   public String getDescription() {
      return super.getDescription();
   }

   @Field(store = Store.YES, analyze = Analyze.NO)
   @Override
   public Date getCreationDate() {
      return super.getCreationDate();
   }
}
