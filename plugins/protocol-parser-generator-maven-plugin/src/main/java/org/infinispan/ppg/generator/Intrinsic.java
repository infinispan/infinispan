package org.infinispan.ppg.generator;

import java.util.List;

public class Intrinsic implements Resolvable {
   private final String qualifiedName;
   private final String varName;
   private final String callCode;
   private final String type;
   final String file;
   final int line;

   Intrinsic(String qualifiedName, String callCode, String type, String file, int line) {
      this.qualifiedName = qualifiedName;
      this.varName = qualifiedName.replaceAll("\\.", "_");
      this.callCode = callCode;
      this.type = type;
      this.file = file;
      this.line = line;
   }

   @Override
   public String qualifiedName() {
      return qualifiedName;
   }

   @Override
   public String sourceName() {
      return varName;
   }

   @Override
   public Machine.State addStates(Machine.State prev, Machine.State target, Machine machine, Grammar grammar, List<RuleDefinition> ruleStack, Reference reference) {
      StringBuilder code = new StringBuilder();
      code.append(varName).append(" = ").append(callCode).append("(buf");
      if (reference.params != null) {
         for (Action action : reference.params) {
            code.append(", ").append(action.code(grammar));
         }
      }
      code.append(')');
      return prev.requireCall(code.toString(), target, ruleStack);
   }

   @Override
   public String analyzeType(Grammar grammar) {
      return type;
   }
}
