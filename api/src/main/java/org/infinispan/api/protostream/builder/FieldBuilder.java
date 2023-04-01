package org.infinispan.api.protostream.builder;

public class FieldBuilder {

   private final MessageBuilder parent;
   private String name;
   private final int number;
   private final boolean required;
   private final String type;

   private IndexedFieldBuilder indexing = null;
   private boolean indexEmbedded = false;

   public FieldBuilder(MessageBuilder parent, String name, int number, boolean required, String type) {
      this.parent = parent;
      this.name = name;
      this.number = number;
      this.required = required;
      this.type = type;
   }

   public MessageBuilder message(String name) {
      return parent.parent.message(name);
   }

   public FieldBuilder required(String name, int number, String type) {
      return parent.required(name, number, type);
   }

   public FieldBuilder optional(String name, int number, String type) {
      return parent.optional(name, number, type);
   }

   public IndexedFieldBuilder basic() {
      indexing = new IndexedFieldBuilder(this, "@Basic");
      return indexing;
   }

   public IndexedFieldBuilder keyword() {
      indexing = new IndexedFieldBuilder(this, "@Keyword");
      return indexing;
   }

   public IndexedFieldBuilder text() {
      indexing = new IndexedFieldBuilder(this, "@Text");
      return indexing;
   }

   public FieldBuilder embedded() {
      indexEmbedded = true;
      return this;
   }

   public String build() {
      return parent.build();
   }

   void write(StringBuilder builder) {
      if (indexing != null) {
         indexing.write(builder);
      } else if (indexEmbedded) {
         ProtoBuf.tab(builder);
         builder.append("/**\n");
         ProtoBuf.tab(builder);
         builder.append(" * @Embedded\n");
         ProtoBuf.tab(builder);
         builder.append(" */\n");
      }

      // optional string name = 1;\n" +
      if (required) {
         ProtoBuf.tab(builder);
         builder.append("required ");
      } else {
         ProtoBuf.tab(builder);
         builder.append("optional ");
      }

      builder.append(type);
      builder.append(" ");
      builder.append(name);
      builder.append(" = ");
      builder.append(number);
      builder.append(";");
      ProtoBuf.blankLine(builder);
   }
}
