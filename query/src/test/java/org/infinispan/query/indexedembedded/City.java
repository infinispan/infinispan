package org.infinispan.query.indexedembedded;

import org.infinispan.api.annotations.indexing.Text;
import org.infinispan.protostream.annotations.ProtoField;

/**
 * @author Sanne Grinovero &lt;sanne@hibernate.org&gt; (C) 2011 Red Hat Inc.
 */
public class City {

   @ProtoField(1)
   public String name;

   @Text
   public String getName() {
      return name;
   }
}
