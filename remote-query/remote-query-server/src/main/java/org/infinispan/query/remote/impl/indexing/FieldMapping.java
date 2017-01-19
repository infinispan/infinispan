package org.infinispan.query.remote.impl.indexing;

import org.hibernate.search.bridge.FieldBridge;
import org.hibernate.search.bridge.LuceneOptions;

/**
 * A mapping from an object field to an index field and the flags that enable indexing, storage and analysis.
 *
 * @author anistor@redhat.com
 * @since 9.0
 */
public final class FieldMapping {

   /**
    * The name of the field in the index.
    */
   private final String name;

   /**
    * Enable indexing.
    */
   private final boolean index;

   private final float boost;

   /**
    * Enable analysis.
    */
   private final boolean analyze;

   /**
    * Enable storage.
    */
   private final boolean store;

   private final boolean sortable;

   /**
    * The name of the analyzer definition.
    */
   private final String analyzer;

   private final String indexNullAs;

   private final LuceneOptions luceneOptions;

   private FieldBridge fieldBridge;

   FieldMapping(String name, boolean index, float boost, boolean analyze, boolean store, boolean sortable,
                String analyzer, String indexNullAs, LuceneOptions luceneOptions) {
      this.name = name;
      this.index = index;
      this.boost = boost;
      this.analyze = analyze;
      this.store = store;
      this.sortable = sortable;
      this.analyzer = analyzer;
      this.indexNullAs = indexNullAs;
      this.luceneOptions = luceneOptions;
   }

   public String getName() {
      return name;
   }

   public boolean index() {
      return index;
   }

   public float boost() {
      return boost;
   }

   public boolean analyze() {
      return analyze;
   }

   public boolean store() {
      return store;
   }

   public boolean sortable() {
      return sortable;
   }

   public String analyzer() {
      return analyzer;
   }

   public String indexNullAs() {
      return indexNullAs;
   }

   public LuceneOptions luceneOptions() {
      return luceneOptions;
   }

   public FieldBridge getFieldBridge() {
      return fieldBridge;
   }

   public void setFieldBridge(FieldBridge fieldBridge) {
      this.fieldBridge = fieldBridge;
   }

   @Override
   public String toString() {
      return "FieldMapping{" +
            "name='" + name + '\'' +
            ", index=" + index +
            ", boost=" + boost +
            ", analyze=" + analyze +
            ", store=" + store +
            ", sortable=" + sortable +
            ", analyzer='" + analyzer + '\'' +
            ", indexNullAs='" + indexNullAs + '\'' +
            ", luceneOptions=" + luceneOptions +
            ", fieldBridge=" + fieldBridge +
            '}';
   }
}
