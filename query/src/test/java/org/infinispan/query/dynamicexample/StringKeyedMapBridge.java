package org.infinispan.query.dynamicexample;

import java.util.Map;

import org.apache.lucene.document.Document;
import org.hibernate.search.bridge.FieldBridge;
import org.hibernate.search.bridge.LuceneOptions;

/**
 * @author Sanne Grinovero &lt;sanne@infinispan.org&gt; (C) 2011 Red Hat Inc.
 */
public class StringKeyedMapBridge implements FieldBridge {

   @Override
   public void set(String name, Object value, Document document, LuceneOptions luceneOptions) {
      Map<String, String> properties = (Map<String, String>) value;
      for (Map.Entry<String, String> entry : properties.entrySet()) {
         luceneOptions.addFieldToDocument(entry.getKey(), entry.getValue(), document);
      }
   }

}
