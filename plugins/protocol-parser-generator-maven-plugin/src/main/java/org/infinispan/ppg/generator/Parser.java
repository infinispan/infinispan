package org.infinispan.ppg.generator;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.StreamTokenizer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import com.github.javaparser.JavaParser;
import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.ast.Modifier;
import com.github.javaparser.ast.NodeList;
import com.github.javaparser.ast.body.ClassOrInterfaceDeclaration;
import com.github.javaparser.ast.body.FieldDeclaration;
import com.github.javaparser.ast.body.MethodDeclaration;
import com.github.javaparser.ast.body.Parameter;
import com.github.javaparser.ast.body.VariableDeclarator;


public class Parser {
   private final List<File> sourceDirectories;
   private final Consumer<String> debug;
   private Set<File> processedFiles = new HashSet<>();
   private Grammar grammar = new Grammar();

   public Parser(Consumer<String> debug, List<File> sourceDirectories) {
      this.debug = debug;
      this.sourceDirectories = sourceDirectories;
   }

   public Grammar load(File file) throws IOException {
      parse(file, false);
      grammar.checkReferences();
      return grammar;
   }

   private void parse(File file, boolean fromInclude) throws IOException {
      if (!processedFiles.add(file)) {
         // prevent recursion in includes
         return;
      }
      StreamTokenizer tokenizer = new StreamTokenizer(new FileReader(file));
      tokenizer.resetSyntax();
      tokenizer.wordChars('a', 'z');
      tokenizer.wordChars('A', 'Z');
      tokenizer.wordChars('0', '9');
      tokenizer.wordChars('_', '_');
      tokenizer.wordChars('.', '.');
      tokenizer.wordChars('$', '$');
      tokenizer.wordChars('@', '@');
      tokenizer.wordChars(128 + 32, 255);
      tokenizer.whitespaceChars(0, ' ');
      tokenizer.slashSlashComments(true);
      tokenizer.slashStarComments(true);
      tokenizer.quoteChar('"');

      String className;
      boolean isRoot = false;
      ParseContext ctx = new ParseContext(tokenizer, file.getName());
      while (ctx.hasNext()) {
         String t = ctx.next();
         switch (t) {
            case "namespace":
               String ns = ctx.next("namespace");
               if (!validateIdentifier(ns)) {
                  throw ctx.fail("Invalid identifier '" + ns + "'!");
               }
               ctx.ns = ns;
               expectSemicolon(ctx);
               break;
            case "class":
               String fqcn = ctx.next("class name");
               grammar.setClassName(fqcn);
               validateClassname(ctx, fqcn);
               t = ctx.next("'extends' or ';'");
               if ("extends".equals(t)) {
                  t = ctx.next("base class name");
                  validateClassname(ctx, t);
                  grammar.setBaseClassName(t);
                  expectSemicolon(ctx);
                  break;
               } else if (";".equals(t)) {
                  break;
               } else {
                  throw ctx.fail("Expected 'extends' or ';', found '" + t + "'");
               }
            case "constants":
               className = processClassName(ctx);
               addConstants(ctx, className, loadClass(ctx, className));
               break;
            case "intrinsics":
               className = processClassName(ctx);
               addIntrinsics(ctx, className, loadClass(ctx, className));
               break;
            case "import":
               grammar.imports.add(processClassName(ctx));
               break;
            case "include":
               String f = ctx.next("filename");
               parse(new File(file.getParentFile(), f), true);
               expectSemicolon(ctx);
               break;
            case "init":
               ctx.next("{");
               grammar.initAction = new Action(ctx.ns, processCode(ctx), ctx.file, ctx.line);
               break;
            case "exceptionally":
               ctx.next("{");
               grammar.exceptionally = new Action(ctx.ns, processCode(ctx), ctx.file, ctx.line);
               break;
            case "deadend":
               ctx.next("{");
               grammar.deadEnd = new Action(ctx.ns, processCode(ctx), ctx.file, ctx.line);
               break;
            case "root":
               isRoot = true;
               break;
            case "|":
               throw ctx.fail("Unexpected '|': extra semicolon before this?");
            default:
               RuleDefinition rule = processRule(ctx, t);
               grammar.addRule(rule);
               if (isRoot && !fromInclude) {
                  if (grammar.root != null) {
                     throw ctx.fail("Second root rule '" + rule.qualifiedName + "', already has defined root '" +
                           grammar.root.qualifiedName + "'");
                  }
                  grammar.root = rule;
                  isRoot = false;
               }
         }
      }
   }

   private void addConstants(ParseContext ctx, String className, ClassOrInterfaceDeclaration clazz) {
      for (FieldDeclaration f : clazz.getFields()) {
         for (VariableDeclarator v : f.getVariables()) {
            if (ctx.ns == null) {
               throw ctx.fail("Namespace for intrinsics from '" + className + " has not been defined.");
            }
            String qualifiedName = ctx.ns + "." + v.getName();
            String sourceName = className + "." + v.getName();
            grammar.addConstant(new Constant(qualifiedName, sourceName, f.getCommonType().asString(), ctx.file, ctx.line));
         }
      }
   }

   private void addIntrinsics(ParseContext ctx, String className, ClassOrInterfaceDeclaration clazz) {
      for (MethodDeclaration m : clazz.getMethods()) {
         if (!m.getModifiers().contains(Modifier.STATIC)) {
            continue;
         }
         NodeList<Parameter> parameterTypes = m.getParameters();
         if (parameterTypes.size() < 1 || !"ByteBuf".equals(parameterTypes.get(0).getTypeAsString())) {
            continue;
         }
         if (ctx.ns == null) {
            throw ctx.fail("Namespace for intrinsics from '" + className + " has not been defined.");
         }
         String name = m.getNameAsString();
         // Drop _ avoiding keyword match
         if (name.endsWith("_")) {
            name = name.substring(0, name.length() - 1);
         }
         String qualifiedName = ctx.ns + "." + name;
         grammar.addIntrinsic(new Intrinsic(qualifiedName, className + "." + m.getNameAsString(), m.getType().asString(), ctx.file, ctx.line));
      }
   }

   private ClassOrInterfaceDeclaration loadClass(ParseContext ctx, String className) {
      Optional<File> classFile = sourceDirectories.stream()
            .map(dir -> new File(dir, className.replace('.', File.separatorChar) + ".java"))
            .filter(file -> {
               debug.accept("Looking up file " + file);
               return file.exists() && file.isFile();
            }).findFirst();
      if (!classFile.isPresent()) {
         throw ctx.fail("Cannot find " + className + " in any of " + sourceDirectories);
      }
      try {
         CompilationUnit compilationUnit = JavaParser.parse(classFile.get());
         int dotIndex = className.lastIndexOf('.');
         String simpleName = dotIndex < 0 ? className : className.substring(dotIndex + 1);

         Optional<ClassOrInterfaceDeclaration> classByName = compilationUnit.getClassByName(simpleName);
         if (!classByName.isPresent()) {
            classByName = compilationUnit.getInterfaceByName(simpleName);
         }
         return classByName.orElseThrow(() -> ctx.fail("Cannot find class " + className + " in " + classFile.get()));
      } catch (FileNotFoundException e) {
         throw ctx.fail("Cannot parse file " + classFile);
      }
   }

   private RuleDefinition processRule(ParseContext ctx, String ruleName) throws IOException {
      int dotIndex = ruleName.lastIndexOf('.');
      RuleDefinition rule;
      if (!validateIdentifier(ruleName)) {
         throw ctx.fail("Invalid identifier '" + ruleName + "'");
      }
      if (dotIndex >= 0) {
         if (dotIndex == ruleName.length() - 1) {
            throw ctx.fail("Rule name cannot be empty!");
         }
         rule = new RuleDefinition(ruleName.substring(0, dotIndex), ruleName.substring(dotIndex + 1), ctx.file, ctx.line);
      } else {
         checkNamespace(ctx, ruleName);
         rule = new RuleDefinition(ctx.ns, ruleName, ctx.file, ctx.line);
      }
      LOOP: for (;;) {
         switch (ctx.next("':', 'returns' or 'switch'")) {
            case ":":
               break LOOP;
            case "returns":
               if (rule.explicitType != null) {
                  throw ctx.fail("Explicit type for rule " + ruleName + " already defined!");
               }
               rule.explicitType = processType(ctx);
               break;
            case "switch":
               if (rule.switchOn != null) {
                  throw ctx.fail("Switch variable for rule " + ruleName + " already defined!");
               }
               rule.switchOn = processReference(ctx, ctx.next("reference"));
               break;
         }
      }
      Branch branch = new Branch(rule, ctx.file, ctx.line);
      while (ctx.hasNext()) {
         String token = ctx.next();
         switch (token) {
            case "{":
               List<String> tokens = processCode(ctx);
               if (ctx.hasNext() && "?".equals(ctx.next())) {
                  branch.sentinel = new Action(ctx.ns, tokens, ctx.file, ctx.line);
               } else {
                  branch.elements.add(new Action(ctx.ns, tokens, ctx.file, ctx.line));
                  ctx.pushBack();
               }
               break;
            case "#":
               token = ctx.next("reference to repetition element");
               Reference counter = processReference(ctx, token);
               branch.elements.add(new Loop(counter, processElement(ctx, ctx.next("element"))));
               break;
            case "|":
            case ";":
               if (branch.elements.isEmpty()) {
                  throw ctx.fail("Empty branch for rule " + rule.qualifiedName);
               }
               rule.branches.add(branch);
               if (";".equals(token)) {
                  return rule;
               }
               branch = new Branch(rule, ctx.file, ctx.line);
               break;
            case ":":
               throw ctx.fail("Unexpected colon; forgot to terminate previous rule?");
            default:
               branch.elements.add(processElement(ctx, token));
               break;
         }
      }
      throw ctx.fail("Unfinished rule " + rule.qualifiedName + " starting on line " + rule.line);
   }

   private String processType(ParseContext ctx) throws IOException {
      String type = ctx.next("type");
      if (!ctx.hasNext()) return type;
      switch (ctx.next()) {
         case "<":
            return consumeUntil(ctx, "<", ">", new StringBuilder(type).append("<"));
         case "[":
            return consumeUntil(ctx, "[", "]", new StringBuilder(type).append("["));
         default:
            ctx.pushBack();
            return type;
      }
   }

   private String consumeUntil(ParseContext ctx, String open, String close, StringBuilder sb) throws IOException {
      int level = 1;
      for (;;) {
         String token = ctx.next(close);
         if (open.equals(token)) {
            ++level;
            sb.append(token);
         } else if (close.equals(token) && --level == 0) {
            return sb.append(token).toString();
         } else {
            if (Character.isLetter(token.charAt(0))) {
               sb.append(' ');
            }
            sb.append(token);
         }
      }
   }

   private List<String> processCode(ParseContext ctx) throws IOException {
      String token;
      List<String> tokens = new ArrayList<>();
      int level = 1;
      for (;;) {
         token = ctx.next("'}'");
         switch (token) {
            case "}":
               if (--level == 0) return tokens;
               else tokens.add("}");
               break;
            case "{":
               ++level;
               // no break;
            default: tokens.add(token);
         }
      }
   }

   private Element processElement(ParseContext ctx, String token) throws IOException {
      if ("(".equals(token)) {
         return processSequence(ctx, ctx.line);
      }
      try {
         if (token.startsWith("0x")) {
               int value = Integer.parseInt(token.substring(2), 16);
               if (value > 255) {
                  throw ctx.fail("Expected value in range 0 .. 255, found " + value);
               }
               return new ByteLiteral(value);
         } else if (token.matches("[0-9]*")) {
               int value = Integer.parseInt(token);
               if (value > 255) {
                  throw ctx.fail("Expected value in range 0 .. 255, found " + value);
               }
               return new ByteLiteral(value);
         }
      } catch (NumberFormatException e) {
         throw ctx.fail("Cannot parse hexadecimal literal '" + token + "'!");
      }
      return processReference(ctx, token);
   }

   private void checkNamespace(ParseContext ctx, String ruleName) {
      if (ctx.ns == null) {
         throw ctx.fail("Default namespace has not been defined but the rule '" + ruleName + "' does not have explicit namespace!");
      }
   }

   private Sequence processSequence(ParseContext ctx, int startLine) throws IOException {
      Sequence sequence = new Sequence();
      while (ctx.hasNext()) {
         String token = ctx.next();
         switch (token) {
            case ")":
               if (sequence.elements.isEmpty()) {
                  throw ctx.fail("Sequence cannot be empty");
               }
               return sequence;
            case "{":
               sequence.elements.add(new Action(ctx.ns, processCode(ctx), ctx.file, ctx.line));
               break;
            case "#":
               token = ctx.next("reference to repetition element");
               Reference counter = processReference(ctx, token);
               sequence.elements.add(new Loop(counter, processElement(ctx, ctx.next("element"))));
               break;
            default:
               sequence.elements.add(processElement(ctx, token));
               break;
         }
      }
      throw ctx.fail("Did not found the end of sequence starting on line " + startLine);
   }

   private Reference processReference(ParseContext ctx, String token) throws IOException {
      if (!validateIdentifier(token)) {
         throw ctx.fail("Expected identifier, found '" + token + "'!");
      }
      int dotIndex = token.lastIndexOf('.');
      Reference r;
      if (dotIndex >= 0) {
         r = new Reference(token, ctx.file, ctx.line);
      } else {
         checkNamespace(ctx, token);
         r = new Reference(ctx.ns + "." + token, ctx.file, ctx.line);
      }
      if (!ctx.hasNext()) {
         return r;
      }
      token = ctx.next();
      if ("[".equals(token)) {
         List<Action> params = new ArrayList<>();
         List<String> tokens = new ArrayList<>();
         int level = 1;
         LOOP: for (;;) {
            token = ctx.next("']'");
            switch (token) {
               case "]":
                  if (--level == 0) break LOOP;
                  else tokens.add("]");
                  break;
               case ",":
                  if (level == 0) {
                     params.add(new Action(ctx.ns, tokens, ctx.file, ctx.line));
                     tokens = new ArrayList<>();
                  } else tokens.add(token);
               case "[":
                  ++level;
                  // no break;
               default: tokens.add(token);
            }
         }
         params.add(new Action(ctx.ns, tokens, ctx.file, ctx.line));
         r.params = params;
      } else {
         ctx.pushBack();
      }
      return r;
   }

   private String processClassName(ParseContext ctx) throws IOException {
      String className = ctx.next("class name");
      validateClassname(ctx, className);
      expectSemicolon(ctx);
      return className;
   }

   private void expectSemicolon(ParseContext ctx) throws IOException {
      String found = ctx.next(";");
      if (!";".equals(found)) {
         throw ctx.fail("Expected ';', found '" + found + "'!");
      }
   }

   private boolean validateIdentifier(String value) {
      return value.matches("[0-9a-zA-Z_][0-9a-zA-Z_.]*");
   }

   private void validateClassname(ParseContext ctx, String value) {
      if (!value.matches("[a-zA-Z_][0-9a-zA-Z_.$]*")) {
         throw ctx.fail("Invalid class name '" + value + "'");
      }
   }

   private static class ParseContext {
      final StreamTokenizer tokenizer;
      final String file;
      String value;
      String prev;
      boolean hasNext = true;
      int line;
      String ns;

      ParseContext(StreamTokenizer tokenizer, String file) throws IOException {
         this.tokenizer = tokenizer;
         this.file = file;
         loadNext(tokenizer);
      }

      void loadNext(StreamTokenizer tokenizer) throws IOException {
         for (;;) {
            switch (tokenizer.nextToken()) {
               case StreamTokenizer.TT_EOF:
                  hasNext = false;
                  return;
               case StreamTokenizer.TT_WORD:
                  value = tokenizer.sval;
                  return;
               case StreamTokenizer.TT_NUMBER:
                  throw new ParserException("Numbers should be parsed as words!");
               case StreamTokenizer.TT_EOL:
                  continue;
               case '"':
                  value = '"' + tokenizer.sval + '"';
                  return;
               default:
                  value = String.valueOf((char) tokenizer.ttype);
                  return;
            }
         }
      }

      boolean hasNext() {
         return hasNext;
      }

      String next() throws IOException {
         prev = value;
         line = tokenizer.lineno();
         loadNext(tokenizer);
         return prev;
      }

      String next(String token) throws IOException {
         if (!hasNext) {
            throw Parser.fail("Expected " + token + ", found EOF!", file, line);
         }
         return next();
      }

      ParserException fail(String message) {
         return Parser.fail(message, file, line);
      }

      void pushBack() {
         value = prev;
         hasNext = true;
         tokenizer.pushBack();
      }
   }

   private static ParserException fail(String message, String file, int line) {
      return new ParserException(file + ":" + line + ": " + message);
   }
}
