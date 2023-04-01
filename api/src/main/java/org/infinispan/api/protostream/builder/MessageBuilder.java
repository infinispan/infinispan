package org.infinispan.api.protostream.builder;

import java.util.ArrayList;
import java.util.List;

public class MessageBuilder {

   final ProtoBuf parent;
   private final String name;

   private final List<FieldBuilder> fields = new ArrayList<>();
   private boolean indexed = false;

   public MessageBuilder(ProtoBuf parent, String name) {
      this.parent = parent;
      this.name = name;
   }

   public MessageBuilder indexed() {
      indexed = true;
      return this;
   }

   public FieldBuilder required(String name, int number, String type) {
      FieldBuilder field = new FieldBuilder(this, name, number, true, type);
      fields.add(field);
      return field;
   }

   public FieldBuilder optional(String name, int number, String type) {
      FieldBuilder field = new FieldBuilder(this, name, number, false, type);
      fields.add(field);
      return field;
   }

   public String build() {
      return parent.build();
   }

   void write(StringBuilder builder) {
      if (indexed) {
         builder.append("/**\n");
         builder.append(" * @Indexed\n");
         builder.append(" */\n");
      }

      builder.append("message ");
      builder.append(name);
      builder.append(" {");
      ProtoBuf.blankLine(builder);

      for (FieldBuilder field : fields) {
         field.write(builder);
      }

      builder.append("}");
      ProtoBuf.blankLine(builder);
   }
}
