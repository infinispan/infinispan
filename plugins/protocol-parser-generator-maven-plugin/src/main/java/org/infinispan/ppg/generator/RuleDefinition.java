package org.infinispan.ppg.generator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class RuleDefinition implements Resolvable {
   final String qualifiedName;
   final String sourceName;
   final String file;
   final int line;
   final List<Branch> branches = new ArrayList<>();
   String explicitType;
   Reference switchOn;

   public RuleDefinition(String ns, String unqualifiedName, String file, int line) {
      this.qualifiedName = ns + "." + unqualifiedName;
      this.sourceName = qualifiedName.replaceAll("\\.", "_");
      this.file = file;
      this.line = line;
   }

   @Override
   public String toString() {
      StringBuilder sb = new StringBuilder().append(String.format("%s:%d: ", file, line))
            .append(qualifiedName).append(" :\n\t");
      Iterator<Branch> it = branches.iterator();
      if (!it.hasNext()) sb.append("<no branches?> ;");
      for (;;) {
         sb.append(it.next()).append("\n\t");
         if (it.hasNext()) {
            sb.append("| ");
         } else {
            sb.append(";\n");
            break;
         }
      }
      return sb.toString();
   }

   public void addReferences(Set<Reference> references) {
      if (switchOn != null) {
         references.add(switchOn);
      }
      for (Branch b : branches) {
         b.addReferences(references);
      }
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
         throw new GeneratorException(reference.file + ":" + reference.line + ": Invalid reference to " + qualifiedName + ", rules cannot have parameters!");
      }
      for (RuleDefinition rule : ruleStack) {
         if (rule == this) {
            StringBuilder sb = new StringBuilder("Illegal recursion: ");
            for (RuleDefinition r : ruleStack) {
               if (r == this) sb.append("***");
               sb.append(r.qualifiedName);
               if (r == this) sb.append("***");
               sb.append(" -> ");
            }
            sb.append(rule.qualifiedName);
            throw new GeneratorException(sb.toString());
         }
      }
      ruleStack.add(this);
      if (switchOn != null) {
         prev.setSwitch(switchOn.sourceName);
      }
      for (Iterator<Branch> iterator = branches.iterator(); iterator.hasNext(); ) {
         Branch b = iterator.next();
         if (b.sentinel == null && iterator.hasNext()) {
            throw new GeneratorException(b.file + ":" + b.line + ": All but the last branches need sentinel predicates!");
         }
         b.addStates(prev, target, machine, grammar, ruleStack);
      }
      RuleDefinition self = ruleStack.remove(ruleStack.size() - 1);
      assert this == self;
      return target;
   }

   public String analyzeType(Grammar grammar) {
      if (explicitType != null) {
         return explicitType;
      }
      String commonType = null;
      for (Branch b : branches) {
         String branchType = b.analyzeType(grammar);
         if ("throw".equals(branchType)) {
            continue;
         } else if (commonType == null) {
            commonType = branchType;
         } else if (!commonType.equals(branchType)) {
            throw new GeneratorException(file + ":" + line + ": rule " + qualifiedName + ": Branches analyzed to different types; " + commonType + " != " + branchType);
         }
      }
      return commonType;
   }
}
