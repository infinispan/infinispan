package org.infinispan.query.dsl.embedded.testdomain;

import java.util.Date;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
public interface Transaction {

   int getId();

   void setId(int id);

   String getDescription();

   void setDescription(String description);

   String getLongDescription();

   void setLongDescription(String longDescription);

   String getNotes();

   void setNotes(String notes);

   int getAccountId();

   void setAccountId(int accountId);

   Date getDate();

   void setDate(Date date);

   double getAmount();

   void setAmount(double amount);

   boolean isDebit();

   void setDebit(boolean isDebit);

   boolean isValid();

   void setValid(boolean isValid);
}
