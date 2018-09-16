package org.infinispan.query.dynamicexample;

import java.util.HashMap;
import java.util.Map;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.FieldBridge;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.Store;

/**
 * @author Sanne Grinovero &lt;sanne@infinispan.org&gt; (C) 2011 Red Hat Inc.
 */
@Indexed
public class DynamicPropertiesEntity {

   private final Map<String,String> properties = new HashMap<>();

   @Field(analyze=Analyze.YES, store=Store.YES)
   @FieldBridge(impl=StringKeyedMapBridge.class)
   public Map<String, String> getProperties() {
      return properties;
   }

   public DynamicPropertiesEntity set(String key, String value) {
      properties.put(key, value);
      return this;
   }

}
