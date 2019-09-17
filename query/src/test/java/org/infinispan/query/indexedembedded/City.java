package org.infinispan.query.indexedembedded;

import java.io.Serializable;

import org.hibernate.search.annotations.Field;
import org.infinispan.protostream.annotations.ProtoField;

/**
 * @author Sanne Grinovero &lt;sanne@hibernate.org&gt; (C) 2011 Red Hat Inc.
 */
public class City implements Serializable {

   @ProtoField(number = 1)
   public @Field String name;
}
