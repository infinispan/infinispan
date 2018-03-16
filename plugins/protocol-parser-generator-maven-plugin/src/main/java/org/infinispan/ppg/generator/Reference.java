package org.infinispan.ppg.generator;

import java.util.List;
import java.util.Set;

public class Reference implements Element {
   final String sourceName;
   final String qualifiedName;
   final String file;
   final int line;
   List<Action> params;

   public Reference(String qualifiedName, String file, int line) {
      this.qualifiedName = qualifiedName;
      this.sourceName = qualifiedName.replaceAll("\\.", "_");
      this.file = file;
      this.line = line;
   }

   @Override
   public String toString() {
      return qualifiedName;
   }

   @Override
   public void addReferences(Set<Reference> references) {
      references.add(this);
   }

   @Override
   public Machine.State addStates(Machine.State prev, Machine.State target, Machine machine, Grammar grammar, List<RuleDefinition> ruleStack) {
      Resolvable resolvable = grammar.qualified.get(qualifiedName);
      return resolvable.addStates(prev, target, machine, grammar, ruleStack, this);
   }

   @Override
   public String analyzeType(Grammar grammar) {
      return grammar.qualified.get(qualifiedName).analyzeType(grammar);
   }

}
