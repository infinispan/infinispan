package org.infinispan.ppg.generator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Machine {
   private static final String INDENT = "\t\t\t";

   private final List<String> imports = new ArrayList<>();
   private final Map<String, String> variables = new HashMap<>();
   private final String pkg;
   private final String simpleName;
   private final String baseClassName;
   private final String initAction;
   private final String exceptionally;
   private final String deadEnd;
   private final int maxSwitchStates;
   private final int userSwitchThreshold;
   private final int switchShift;
   private final boolean passContext;
   private List<State> states = new ArrayList<>();

   public Machine(String pkg, String simpleName, String baseClassName, String initAction, String exceptionally, String deadEnd, int maxSwitchStates, int userSwitchThreshold, boolean passContext) {
      this.pkg = pkg;
      this.simpleName = simpleName;
      this.baseClassName = baseClassName;
      this.initAction = initAction;
      this.exceptionally = exceptionally;
      this.deadEnd = deadEnd;
      this.maxSwitchStates = Integer.highestOneBit(maxSwitchStates - 1) << 1;
      this.userSwitchThreshold = userSwitchThreshold;
      this.switchShift = 32 - Integer.numberOfLeadingZeros(maxSwitchStates - 1);
      this.passContext = passContext;
   }

   public State addState(List<RuleDefinition> ruleStack) {
      State s = new State(stackComment(ruleStack));
      s.id = states.size();
      states.add(s);
      return s;
   }

   public String buildSource() {
      StringBuilder sb = new StringBuilder();
      StringBuilder extraMethods = new StringBuilder();

      sb.append("package ").append(pkg).append(";\n");
      sb.append("import java.util.List;\n");
      sb.append("import io.netty.buffer.ByteBuf;\n");
      sb.append("import io.netty.channel.ChannelHandlerContext;\n");
      if (baseClassName == null) {
         sb.append("import io.netty.handler.codec.ByteToMessageDecoder;\n");
      }
      for (String imp : imports) {
         sb.append("import ").append(imp).append(";\n");
      }
      sb.append("\npublic class ").append(simpleName).append(" extends ");
      sb.append(baseClassName != null ? baseClassName : "ByteToMessageDecoder").append(" {\n");
      sb.append("\tprivate int state;\n");
      sb.append("\tprivate int requestBytes;\n\n");
      for (Map.Entry<String, String> var : variables.entrySet()) {
         sb.append("\tprivate ").append(var.getValue()).append(' ').append(var.getKey()).append(";\n");
      }
      sb.append("\n");
      if (initAction != null) {
         sb.append(prettyPrint(initAction, 1)).append("\n");
      }
      int numLevels = (32 - Integer.numberOfLeadingZeros(states.size() - 1)) / switchShift + 1;
      sb.append("\t@Override\n");
      sb.append("\tpublic void decode(ChannelHandlerContext ctx, ByteBuf buf, List<Object> out) throws Exception {\n");
      sb.append("\t\tint pos = buf.readerIndex();\n");
      sb.append("\t\ttry {\n");
      sb.append("\t\t\twhile (switch").append(numLevels > 1 ? (numLevels - 1) + "_" : "").append("0(");
      if (passContext) sb.append("ctx, ");
      sb.append("buf));\n");
      sb.append("\t\t} catch (Throwable t) {\n");
      if (exceptionally == null) {
         sb.append("\t\t\tthrow t;\n");
      } else {
         sb.append("\t\t\texceptionally(t);\n");
      }
      sb.append("\t\t} finally {\n");
      sb.append("\t\t\trequestBytes += buf.readerIndex() - pos;\n");
      sb.append("\t\t}\n");
      sb.append("\t}\n\n");

      // We issue multiple level of switches to avoid excessively large methods
      writeSwitch(sb, numLevels - 1, 0);

      int switchCounter = 0;
      for (State state : states) {
         if (state.id % maxSwitchStates == 0) {
            if (switchCounter != 0) {
               sb.append("\t\t}\n\t\treturn true;\n\t}\n\n");
            }
            sb.append("\tprivate boolean switch").append(switchCounter++).append('(');
            if (passContext) {
               sb.append("ChannelHandlerContext ctx, ");
            }
            sb.append("ByteBuf buf) throws Exception {\n");
            sb.append("\t\tbyte b;\n");
            sb.append("\t\tint pos;\n");
            sb.append("\t\tswitch (state) {\n");
         }
         sb.append("\t\tcase ").append(state.id).append(": \n");
         state.toSource(sb, extraMethods);
      }
      sb.append("\t\t}\n\t\treturn true;\n\t}\n\n");

      sb.append("\tprivate void deadEnd() {\n");
      if (deadEnd != null) {
         sb.append(prettyPrint(deadEnd, 2)).append("\n");
      } else {
         sb.append("\t\tthrow new IllegalArgumentException();\n");
      }
      sb.append("\t}\n\n");
      if (exceptionally != null) {
         sb.append("\tprivate void exceptionally(Throwable t) throws Exception {\n");
         sb.append(prettyPrint(exceptionally, 2)).append("\n");
         sb.append("\t}\n\n");
      }
      sb.append("\tprivate void reset() {\n");
      sb.append("\t\trequestBytes = 0;");
      for (Map.Entry<String, String> var : variables.entrySet()) {
         sb.append("\t\t").append(var.getKey()).append(" = ").append(defaultFor(var.getValue())).append(";\n");
      }
      sb.append("\t}\n\n");
      sb.append("\tpublic int requestBytes() {\n");
      sb.append("\t\treturn requestBytes;\n");
      sb.append("\t}\n");
      if (extraMethods.length() > 0) {
         sb.append("\n").append(extraMethods);
      }
      sb.append("}\n");
      return sb.toString();
   }

   private String prettyPrint(String code, int indent) {
      String[] lines = code.split("\n", 0);
      StringBuilder sb = new StringBuilder();
      for (int ln = 0; ln < lines.length; ln++) {
         String line = lines[ln];
         int addIndent = 0;
         for (int i = 0; i < line.length(); ++i) {
            switch (line.charAt(i)) {
               case '{':
                  addIndent++;
                  break;
               case '}':
                  indent--;
                  break;
            }
         }
         for (int i = 0; i < indent; ++i) sb.append('\t');
         sb.append(line.trim());
         indent += addIndent;
         if (ln != lines.length - 1) sb.append('\n');
         // add newline between methods
         if (indent == 1 && line.trim().equals("}")) sb.append('\n');
      }
      return sb.toString();
   }

   private void writeSwitch(StringBuilder sb, int level, int offset) {
      if (level < 1) {
         return;
      }
      sb.append("\tprivate boolean switch").append(level).append('_').append(offset).append('(');
      if (passContext) sb.append("ChannelHandlerContext ctx, ");
      sb.append("ByteBuf buf) throws Exception {\n");
      sb.append("\t\tswitch (state >> ").append(level * switchShift).append(") {\n");
      for (int s = 0; s < maxSwitchStates && ((s + offset) << (level * switchShift)) < states.size(); ++s) {
         sb.append("\t\tcase ").append(s).append(": return switch");
         if (level > 1) {
            sb.append(level - 1).append('_').append(offset);
         } else {
            sb.append(s + offset);
         }
         sb.append('(');
         if (passContext) sb.append("ctx, ");
         sb.append("buf);\n");
      }
      sb.append("\t\tdefault: throw new IllegalStateException();\n");
      sb.append("\t\t}\n\t}\n\n");
      for (int s = 0; s < maxSwitchStates && ((s + offset) << (level * switchShift)) < states.size(); ++s) {
         writeSwitch(sb, level - 1, (offset << switchShift) + s);
      }
   }

   private String defaultFor(String type) {
      switch (type) {
         case "int":
         case "long":
         case "byte":
         case "short":
         case "double":
         case "float":
         case "char":
            return "0";
         case "boolean":
            return "false";
         default:
            return "null";
      }
   }

   public void addVariable(String type, String name) {
      variables.put(name, type);
   }

   public void addImport(String className) {
      imports.add(className);
   }

   /**
    * Try to inline all states that contain only action and state shift.
    */
   public void contractStates() {
      boolean contracted;
      do {
         contracted = false;
         for (Iterator<State> iterator = states.iterator(); iterator.hasNext(); ) {
            State s = iterator.next();
            if (s.links.size() == 1 && s.links.get(0).type == LinkType.BACKTRACK) {
               Link l = s.links.get(0);
               boolean contractedNow = false;
               for (State s2 : states) {
                  for (Link l2 : s2.links) {
                     if (l2.next == s && l2.type != LinkType.SENTINEL) {
                        l2.code = l2.code + "\n" + l.code;
                        l2.next = l.next;
                        contractedNow = true;
                     }
                  }
               }
               if (contractedNow) {
                  iterator.remove();
                  contracted = true;
               }
            }
         }
      } while (contracted);
      // renumber
      for (int i = 0; i < states.size(); ++i) {
         states.get(i).id = i;
      }
   }

   private static String stackComment(List<RuleDefinition> ruleStack) {
      return ruleStack.stream().map(r -> r.qualifiedName).collect(Collectors.joining("/"));
   }

   class State {
      private final String comment;
      private List<Link> links = new ArrayList<>();
      private int id;
      private String switchOn;

      private State(String comment) {
         this.comment = comment;
      }

      public State requireReadByte(String value, State target) {
         Link link = new Link(LinkType.ACTION, target);
         link.code = "if (!buf.isReadable()) return;\n" +
                     "b = buf.readByte();\n" +
                     "if ((0xFF & b) != " + value + ") deadEnd();";
         links.add(link);
         return link.next;
      }

      public State requireCall(String call, State target, List<RuleDefinition> ruleStack) {
         Link link = new Link(LinkType.ACTION, target);
         link.code = "pos = buf.readerIndex();\n" +
               call + ";\n" +
               "if (buf.readerIndex() == pos) return false;";
         links.add(link);
         return link.next;
      }

      public State addSentinel(String sentinel, State target) {
         Link link = new Link(LinkType.SENTINEL, target);
         link.code = sentinel;
         links.add(link);
         return link.next;
      }

      public State addAction(String action, State target) {
         Link link = new Link(LinkType.ACTION, target);
         link.code = action;
         links.add(link);
         return link.next;
      }

      public State addBacktrack(String action, State target) {
         Link link = new Link(LinkType.BACKTRACK, target);
         link.code = action;
         links.add(link);
         return link.next;
      }

      void toSource(StringBuilder sb, StringBuilder extraMethods) {
         sb.append(INDENT).append("// ").append(comment).append("\n");
         if (links.isEmpty()) {
            sb.append(INDENT).append("deadEnd();\n");
            return;
         }

         String indent = INDENT;
         if (switchOn != null) {
            if (links.size() > userSwitchThreshold) {
               sb.append(indent).append("return userSwitch").append(id).append("();\n");
               extraMethods.append("\tprivate boolean userSwitch").append(id).append("() throws Exception {\n");
               sb = extraMethods;
               indent = "\t\t";
            }
            sb.append(indent).append("switch (").append(switchOn).append(") {\n");
         }
         for (Iterator<Link> iterator = links.iterator(); iterator.hasNext(); ) {
            Link link = iterator.next();
            if (link.next != null && link.next != states.get(link.next.id)) {
               throw new IllegalStateException("Target state was deleted!");
            }
            // all but the last links must be sentinels
            if (link.type == LinkType.SENTINEL) {
               if (switchOn == null) {
                  sb.append(indent).append("if (").append(link.code).append(") {\n");
               } else {
                  sb.append(indent).append("case ").append(link.code).append(": \n");
               }
               if (link.next != null) {
                  sb.append(indent).append("\tstate = ").append(link.next.id).append(";\n");
                  sb.append(indent).append("\treturn true;\n");
               }
               if (switchOn == null) {
                  sb.append(indent).append("}\n");
               }
            } else {
               if (switchOn != null) {
                  sb.append(indent).append("default: \n");
                  indent = indent + "\t";
               }
               sb.append(indent).append(link.code.replaceAll("\n", "\n" + indent)).append("\n");
               if (link.next != null) {
                  sb.append(indent).append("state = ").append(link.next.id).append(";\n");
                  if (switchOn != null) {
                     indent = indent.substring(0, indent.length() - 1);
                     sb.append(indent).append("}\n");
                  }
                  if (link.next.id == id + 1) {
                     sb.append(indent).append("// fallthrough\n");
                  } else {
                     sb.append(indent).append("return true;\n");
                  }
               } else if (switchOn != null) {
                  indent = indent.substring(0, indent.length() - 1);
                  sb.append(indent).append("}\n");
               }
            }
            if (iterator.hasNext() && link.type != LinkType.SENTINEL) {
               throw new GeneratorException("Branching without a sentinel");
            } else if (!iterator.hasNext() && link.type == LinkType.SENTINEL) {
               if (switchOn != null) {
                  sb.append(indent).append("default:\n");
                  sb.append(indent).append("\texceptionally(new IllegalArgumentException('")
                        .append(switchOn).append(" + \"' does not match any of the switch cases.\"));\n");
                  sb.append("}\n");
               } else {
                  sb.append(indent).append("deadEnd();\n");
                  sb.append(indent).append("return true;\n");
               }
            }
         }
         if (sb == extraMethods) {
            extraMethods.append("\t}\n\n");
         }
      }

      public void setSwitch(String var) {
         switchOn = var;
      }
   }

   class Link {
      final LinkType type;
      String code;
      // state can be null when the code throws an exception
      State next;

      Link(LinkType type, State target) {
         this.type = type;
         next = target;
      }
   }

   enum LinkType {
      ACTION,
      SENTINEL,
      BACKTRACK
   }
}
