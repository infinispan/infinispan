package org.infinispan.query.dsl.embedded.testdomain;

import java.io.Serializable;
import java.util.Date;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
public interface Account extends Serializable {

   int getId();

   void setId(int id);

   String getDescription();

   void setDescription(String description);

   Date getCreationDate();

   void setCreationDate(Date creationDate);
}
