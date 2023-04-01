package org.infinispan.api.protostream.builder;

import java.util.ArrayList;
import java.util.List;

public final class ProtoBuf {

   public static ProtoBuf builder() {
      return new ProtoBuf();
   }

   private final List<MessageBuilder> messages = new ArrayList<>();
   private String packageName;

   private ProtoBuf() {
   }

   public ProtoBuf packageName(String packageName) {
      this.packageName = packageName;
      return this;
   }

   public MessageBuilder message(String name) {
      MessageBuilder message = new MessageBuilder(this, name);
      messages.add(message);
      return message;
   }

   public String build() {
      StringBuilder builder = new StringBuilder();
      builder.append("syntax = \"proto2\";");
      ProtoBuf.blankLine(builder);

      if (packageName != null && !packageName.isBlank()) {
         builder.append("package ");
         builder.append(packageName);
         builder.append(";");
         ProtoBuf.blankLine(builder);
      }

      for (MessageBuilder message : messages) {
         message.write(builder);
      }
      return builder.toString();
   }

   static void blankLine(StringBuilder builder) {
      builder.append("\n\n");
   }

   static void tab(StringBuilder builder) {
      builder.append("  ");
   }
}
