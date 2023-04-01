package org.infinispan.api.protostream.builder;

public class IndexedFieldBuilder {

   private final FieldBuilder parent;

   // TODO ISPN-14724 Create subclasses for each indexing type
   //   So that we can limit the options here accordingly.
   private final String indexing;

   private boolean custom = false;

   private Boolean searchable;
   private Boolean sortable;
   private Boolean projectable;
   private Boolean aggregable;
   private String indexNullAs;
   private String analyzer;
   private String searchAnalyzer;
   private String normalizer;

   public IndexedFieldBuilder(FieldBuilder parent, String indexing) {
      this.parent = parent;
      this.indexing = indexing;
   }

   public MessageBuilder message(String name) {
      return parent.message(name);
   }

   public FieldBuilder required(String name, int number, String type) {
      return parent.required(name, number, type);
   }

   public FieldBuilder optional(String name, int number, String type) {
      return parent.optional(name, number, type);
   }

   public IndexedFieldBuilder searchable(boolean value) {
      custom = true;
      this.searchable = value;
      return this;
   }

   public IndexedFieldBuilder sortable(boolean value) {
      custom = true;
      this.sortable = value;
      return this;
   }

   public IndexedFieldBuilder projectable(boolean value) {
      custom = true;
      this.projectable = value;
      return this;
   }

   public IndexedFieldBuilder aggregable(boolean value) {
      custom = true;
      this.aggregable = value;
      return this;
   }

   public IndexedFieldBuilder indexNullAs(String value) {
      custom = true;
      this.indexNullAs = value;
      return this;
   }

   public IndexedFieldBuilder analyzer(String value) {
      custom = true;
      this.analyzer = value;
      return this;
   }

   public IndexedFieldBuilder searchAnalyzer(String value) {
      custom = true;
      this.searchAnalyzer = value;
      return this;
   }

   public IndexedFieldBuilder normalizer(String value) {
      custom = true;
      this.normalizer = value;
      return this;
   }

   void write(StringBuilder builder) {
      ProtoBuf.tab(builder);
      builder.append("/**\n");
      ProtoBuf.tab(builder);
      builder.append(" * ");
      builder.append(indexing);

      writeCustomization(builder);

      builder.append("\n");
      ProtoBuf.tab(builder);
      builder.append(" */\n");
   }

   private void writeCustomization(StringBuilder builder) {
      if (!custom) {
         return;
      }

      builder.append("(");
      boolean first = true;

      if (searchable != null) {
         writeAttribute(builder, "searchable", searchable, first);
         first = false;
      }
      if (sortable != null) {
         writeAttribute(builder, "sortable", sortable, first);
         first = false;
      }
      if (projectable != null) {
         writeAttribute(builder, "projectable", projectable, first);
         first = false;
      }
      if (aggregable != null) {
         writeAttribute(builder, "aggregable", aggregable, first);
         first = false;
      }
      if (indexNullAs != null) {
         writeAttribute(builder, "indexNullAs", indexNullAs, first);
         first = false;
      }
      if (analyzer != null) {
         writeAttribute(builder, "analyzer", analyzer, first);
         first = false;
      }
      if (searchAnalyzer != null) {
         writeAttribute(builder, "searchAnalyzer", searchAnalyzer, first);
         first = false;
      }
      if (normalizer != null) {
         writeAttribute(builder, "normalizer", normalizer, first);
      }

      builder.append(")");
   }

   private void writeAttribute(StringBuilder builder, String attribute, Object value, boolean first) {
      if (!first) {
         builder.append(", ");
      }
      builder.append(attribute);
      builder.append("=");

      if (value instanceof String) {
         value = "\"" + value + "\"";
      }
      builder.append(value);
   }
}
