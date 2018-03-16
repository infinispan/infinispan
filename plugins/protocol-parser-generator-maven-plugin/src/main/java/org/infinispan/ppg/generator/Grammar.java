package org.infinispan.ppg.generator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Grammar {
   final Map<String, Resolvable> qualified = new HashMap<>();
   final List<RuleDefinition> rules = new ArrayList<>();
   final List<String> imports = new ArrayList<>();
   final List<Constant> constants = new ArrayList();
   final List<Intrinsic> intrinsics = new ArrayList();
   String pkg;
   String simpleName;
   String baseClassName;
   RuleDefinition root;
   Action initAction;
   Action exceptionally;
   Action deadEnd;

   public void checkReferences() {
      Set<Reference> references = new HashSet<>();
      for (RuleDefinition rule : rules) {
         rule.addReferences(references);
      }
      for (Reference reference : references) {
         if (!qualified.containsKey(reference.qualifiedName)) {
            throw new ParserException(reference.file + ":" + reference.line + ": Reference to unknown element '" + reference.qualifiedName + "'");
         }
      }
   }

   public Machine build(int maxSwitchStates, int userSwitchThreshold, boolean passContext) {
      Machine machine = new Machine(pkg, simpleName, baseClassName,
            initAction == null ? null : initAction.code(this),
            exceptionally == null ? null : exceptionally.code(this),
            deadEnd == null ? null : deadEnd.code(this), maxSwitchStates, userSwitchThreshold, passContext);
      imports.forEach(machine::addImport);

      for (RuleDefinition rule : rules) {
         String type = rule.analyzeType(this);
         if (!"void".equals(type)) {
            machine.addVariable(type, rule.sourceName);
         }
      }
      for (Intrinsic intrinsic : intrinsics) {
         machine.addVariable(intrinsic.analyzeType(this), intrinsic.sourceName());
      }

      ArrayList<RuleDefinition> ruleStack = new ArrayList<>();
      Machine.State initialState = machine.addState(ruleStack);
      Machine.State afterReset = initialState.addAction("reset(); ", machine.addState(ruleStack));
      root.addStates(afterReset, initialState, machine, this, ruleStack, new Reference("<root>", root.file, root.line));
      machine.contractStates();
      return machine;
   }

   public void addRule(RuleDefinition rule) {
      rules.add(rule);
      addResolvable(rule, rule.file, rule.line);
   }

   public void addIntrinsic(Intrinsic intrinsic) {
      intrinsics.add(intrinsic);
      addResolvable(intrinsic, intrinsic.file, intrinsic.line);
   }

   public void addConstant(Constant constant) {
      constants.add(constant);
      addResolvable(constant, constant.file, constant.line);
   }

   public void addResolvable(Resolvable resolvable, String file, int line) {
      Resolvable prev = qualified.put(resolvable.qualifiedName(), resolvable);
      if (prev != null) {
         throw new ParserException(file + ":" + line + ": Duplicate definition for "
               + resolvable.qualifiedName() + ", already defined: " + prev);
      }
   }

   public String getPackage() {
      return pkg;
   }

   public String getSimpleName() {
      return simpleName;
   }

   public void setClassName(String fqcn) {
      if (pkg != null || simpleName != null) {
         throw new ParserException("Class name already set: package=" + pkg + ", name=" + simpleName + ", now got " + fqcn);
      }
      int dotIndex = fqcn.lastIndexOf('.');
      if (dotIndex < 0) {
         throw new ParserException("Expected qualified class name, found '" + fqcn + "'");
      }
      pkg = fqcn.substring(0, dotIndex);
      simpleName = fqcn.substring(dotIndex + 1);
      if (pkg.isEmpty() || simpleName.isEmpty()) {
         throw new ParserException("Invalid class name '" + fqcn + "'");
      }
   }

   public void setBaseClassName(String baseClassName) {
      this.baseClassName = baseClassName;
   }
}
