package org.infinispan.ppg.generator;

import java.util.List;

class Constant implements Resolvable {
   private final String qualifiedName;
   private final String sourceName;
   private final String type;
   final String file;
   final int line;

   public Constant(String qualifiedName, String sourceName, String type, String file, int line) {
      this.qualifiedName = qualifiedName;
      this.sourceName = sourceName;
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
      return sourceName;
   }

   @Override
   public Machine.State addStates(Machine.State prev, Machine.State target, Machine machine, Grammar grammar, List<RuleDefinition> ruleStack, Reference reference) {
      if (reference.params != null) {
         throw new GeneratorException(reference.file + ":" + reference.line + ": Invalid reference to " + sourceName + ", constants cannot have parameters!");
      }
      return prev.requireReadByte(sourceName(), target);
   }

   @Override
   public String analyzeType(Grammar grammar) {
      return type;
   }

   @Override
   public String toString() {
      return file + ":" + line + ": " + qualifiedName + " -> " + sourceName();
   }
}
