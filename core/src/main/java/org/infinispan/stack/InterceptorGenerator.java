package org.infinispan.stack;

import javassist.*;
import javassist.bytecode.*;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Creates a functionally equivalent copy of a class hierarchy. The calls passing the command and context
 * to the next interceptor are transformed into invocations of the target method (circumventing
 * the acceptor-visitor pattern with dynamic dispatch). Virtual calls within the hierarchy are still allowed
 * since the new hierarchy is linear and CHA (class hierarchy analysis) in compiler can optimize the virtual
 * calls easily.
 *
 * Uses {@link NestedClassGenerator} to copy inner classes as well - these contain/capture reference to the
 * original interceptor, therefore we have to use the copy as well.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
class InterceptorGenerator {
   private static final Log log = LogFactory.getLog(InterceptorGenerator.class);
   private static final AtomicLong classCounter = new AtomicLong();
   private static final String OPT_SUFFIX = "$Opt";
   private static final String SYNC_SUFFIX = "$sync";
   private static final String SYNC_ROOT = "$syncRoot$";

   static final String REPLACE_TABLE = "$replaceTable$";
   static final String OBJECT_CLASS_NAME = Object.class.getName();
   static final String STRING_ARRAY_DESCRIPTOR = "[Ljava/lang/String;";
   static final String STRING_ARRAY_TYPE = "java.lang.String[]";

   private final ClassPool classPool = ClassPool.getDefault();
   private final CtClass oldInterceptor;
   private final Map<String, CtClass> newInterceptorsByOldName = new HashMap<>();
   private final List<CtClass> newInterceptorList = new ArrayList<>();
   private final CtClass nextInterceptor;
   private final Map<String, String> syntheticNextMethods = new HashMap<>();
   private final StackOptimizer so;
   private final CtClass nextDelegator;
   private final Set<Inner> oldInnerClasses = new HashSet<>();
   private final List<CtClass> newInnerClasses = new ArrayList<>();
   private final long uniqueId = classCounter.getAndIncrement();

   private Map<CtClass, List<BootstrapMethodsAttribute.BootstrapMethod>> newBootstrapMethods = new HashMap<>();
   private CtField newNextField = null;
   private CtClass baseInterceptor;
   private CtClass topmostInterceptor;
   private CtClass nextMethodInterceptor;
   private Map<String, String> replacedClassNames = new HashMap<>();
   private Map<String, String> replacedSlashedNames = new HashMap<>();
   private int sinkMethodCounter;
   private Class topmostJavaInterceptor;

   public InterceptorGenerator(StackOptimizer so, CtClass oldInterceptor, CtClass nextInterceptor, CtClass nextDelegator) {
      this.so = so;
      this.oldInterceptor = oldInterceptor;
      this.nextInterceptor = nextInterceptor;
      this.nextDelegator = nextDelegator;
   }

   public void run() throws NotFoundException, CannotCompileException, IOException, BadBytecode {
      createClasses();
      createFieldsAndConstructors();
      createNestedClasses();
      createStaticVisitorMethods();
      createMethods();
   }

   /**
    * Find the classes in the hierarchy, and create counterparts
    *
    * @throws NotFoundException
    * @throws CannotCompileException
    */
   private void createClasses() throws NotFoundException, CannotCompileException {
      for (CtClass ctClass = oldInterceptor; ctClass != null && !ctClass.getName().equals(OBJECT_CLASS_NAME); ctClass = ctClass.getSuperclass()) {
         CtClass newInterceptor = classPool.makeClass(ctClass.getName() + OPT_SUFFIX + uniqueId);
         Helper.removeAttribute(newInterceptor.getClassFile().getAttributes(), SourceFileAttribute.tag);
         newInterceptor.getClassFile().addAttribute(new SourceFileAttribute(newInterceptor.getClassFile().getConstPool(), ctClass.getClassFile().getSourceFile()));
         newInterceptor.getClassFile().setMajorVersion(ClassFile.JAVA_8);
         newInterceptorsByOldName.put(ctClass.getName(), newInterceptor);
         newInterceptorList.add(newInterceptor);

         replacedClassNames.put(ctClass.getName(), newInterceptor.getName());
         replacedSlashedNames.put(ctClass.getName().replace('.', '/'), newInterceptor.getName().replace('.', '/'));
         Queue<CtClass> queue = new ArrayDeque<>(Arrays.asList(ctClass.getNestedClasses()));
         for (CtClass innerClass; (innerClass = queue.poll()) != null; ) {
            // If the inner class is also an interceptor, let's ignore it
            if (innerClass.subtypeOf(so.interceptorClass)) {
               continue;
            }
            queue.addAll(Arrays.asList(innerClass.getNestedClasses()));
            replacedClassNames.put(innerClass.getName(), newInterceptor.getName() + "$" + innerClass.getSimpleName());
            replacedSlashedNames.put(innerClass.getName().replace('.', '/'), (newInterceptor.getName() + "$" + innerClass.getSimpleName()).replace('.', '/'));

            // Create the class ahead to allow referencing it by name later with cyclic class dependencies
            // we cannot create non-static inner class with javassist
            CtClass newInnerClass = newInterceptor.makeNestedClass(innerClass.getSimpleName(), true);
            newInnerClass.getClassFile().setMajorVersion(ClassFile.JAVA_8);
            newInnerClasses.add(newInnerClass);
            oldInnerClasses.add(new InterceptorGenerator.Inner(innerClass, newInnerClass));
         }
      }
      // Link superclasses
      for (int i = 0; i < newInterceptorList.size() - 1; ++i) {
         newInterceptorList.get(i).setSuperclass(newInterceptorList.get(i + 1));
      }
      for (int i = 1; i < newInterceptorList.size(); ++i) {
         CtClass ctClass = newInterceptorList.get(i);
         ctClass.setModifiers(ctClass.getModifiers() | Modifier.ABSTRACT);
      }
      topmostInterceptor = newInterceptorList.get(0);
      baseInterceptor = newInterceptorList.get(newInterceptorList.size() - 1);
   }

   private void createFieldsAndConstructors() throws CannotCompileException, NotFoundException {
      // replaceTable is used from Helper#dynamicCopy()
      CtField replaceTable = new CtField(classPool.get(STRING_ARRAY_TYPE), REPLACE_TABLE, baseInterceptor);
      replaceTable.setModifiers(Modifier.PUBLIC | Modifier.FINAL | Modifier.STATIC);
      baseInterceptor.addField(replaceTable);
      // syncRoot is the original interceptor, see #generateSynchronizationMethod()
      CtField syncRoot = new CtField(oldInterceptor, SYNC_ROOT, baseInterceptor);
      syncRoot.setModifiers(Modifier.PROTECTED | Modifier.FINAL);
      baseInterceptor.addField(syncRoot);

      // Second iteration: copy fields and create constructors/initializers
      for (CtClass ctClass = oldInterceptor; ctClass != null && !ctClass.getName().equals(OBJECT_CLASS_NAME); ctClass = ctClass.getSuperclass()) {
         CtClass[] ctorParamsArray = nextInterceptor != null ?
               new CtClass[] { oldInterceptor, nextInterceptor } : new CtClass[] { oldInterceptor };

         CtClass newInterceptor = newInterceptorsByOldName.get(ctClass.getName());
         Bytecode ctorCode = new Bytecode(newInterceptor.getClassFile().getConstPool());
         ctorCode.addAload(0);

         Bytecode staticCtorCode = new Bytecode(newInterceptor.getClassFile().getConstPool());

         if (newInterceptor == baseInterceptor) {
            generateReplaceTable(staticCtorCode);
            // store old interceptor instance as synchronization item
            ctorCode.addInvokespecial(newInterceptor.getSuperclass(), "<init>", CtClass.voidType, Helper.EMPTY_PARAMS);
            ctorCode.addAload(0);
            ctorCode.addAload(1);
            ctorCode.addPutfield(newInterceptor, SYNC_ROOT, Descriptor.of(oldInterceptor));
         } else {
            ctorCode.addLoadParameters(ctorParamsArray, 1);
            ctorCode.addInvokespecial(newInterceptor.getSuperclass(), "<init>", CtClass.voidType, ctorParamsArray);
         }

         for (CtField field : ctClass.getDeclaredFields()) {
            CtField newField;
            int modifiers = field.getModifiers();

            if (ctClass.getName().equals(so.nextFieldClass.getName()) && so.nextFieldName.equals(field.getName())) {
               nextMethodInterceptor = newInterceptor;
               if (nextInterceptor == null) {
                  continue;
               }
               modifiers = Modifier.PROTECTED | Modifier.FINAL;
               newField = new CtField(nextInterceptor, field.getName(), newInterceptor);
               newNextField = newField;
               ctorCode.addAload(0);
               ctorCode.addAload(2);
               ctorCode.addPutfield(newInterceptor, field.getName(), Descriptor.of(nextInterceptor));
            } else {
               String newFieldType = replacedClassNames.get(field.getType().getName());
               newField = new CtField(newFieldType != null ? classPool.get(newFieldType) : field.getType(), field.getName(), newInterceptor);
               if (Modifier.isStatic(modifiers)) {
                  Helper.addCopyField(staticCtorCode, field, newField, newInterceptor, -1, baseInterceptor);
               } else {
                  Helper.addCopyField(ctorCode, field, newField, newInterceptor, 0, baseInterceptor);
               }
            }

            newField.setModifiers(modifiers);
            newInterceptor.addField(newField);
         }

         staticCtorCode.addReturn(CtClass.voidType);
         staticCtorCode.setMaxLocals(true, Helper.EMPTY_PARAMS, 0);

         CtConstructor staticCtor = new CtConstructor(Helper.EMPTY_PARAMS, newInterceptor);
         staticCtor.getMethodInfo().setName("<clinit>");
         staticCtor.setModifiers(Modifier.PUBLIC | Modifier.STATIC);
         staticCtor.getMethodInfo().setCodeAttribute(staticCtorCode.toCodeAttribute());
         newInterceptor.addConstructor(staticCtor);

         CtConstructor ctor = new CtConstructor(ctorParamsArray, newInterceptor);
         ctor.setModifiers(Modifier.PUBLIC);
         ctorCode.addReturn(CtClass.voidType);
         ctorCode.setMaxLocals(false, ctorParamsArray, 0);
         CodeAttribute codeAttribute = ctorCode.toCodeAttribute();
         ctor.getMethodInfo().setCodeAttribute(codeAttribute);
         newInterceptor.addConstructor(ctor);
      }
   }

   private void createNestedClasses() throws NotFoundException, CannotCompileException, IOException, BadBytecode {
      // Copy all inner classes (these usually reference the outer class)
      Queue<Inner> queue = new ArrayDeque<>(oldInnerClasses);
      Set<Inner> unprocessed = new HashSet<>(oldInnerClasses);
      for (InterceptorGenerator.Inner inner; (inner = queue.poll()) != null; ) {
         // we have to process superclasses first to be able to reference new fields
         if (unprocessed.contains(inner.oldClass.getSuperclass())) {
            queue.add(inner);
            continue;
         }
         NestedClassGenerator nestedClassGenerator = new NestedClassGenerator(classPool, newInterceptorsByOldName,
               baseInterceptor, inner.oldClass, inner.newClass, replacedClassNames, replacedSlashedNames);
         nestedClassGenerator.run();
         unprocessed.remove(inner);
      }
   }

   private void createStaticVisitorMethods() throws NotFoundException, CannotCompileException {
      // Add static methods (called through invokestatic) that only delegate to the concrete implementations
      // using invokespecial
      for (CtMethod visitorMethod : so.visitorClass.getDeclaredMethods()) {
         CtClass[] parameterTypes = visitorMethod.getParameterTypes();
         int commandIndex = getCommandIndex(parameterTypes);
         if (commandIndex < 0) {
            throw new IllegalArgumentException();
         }
         CtClass commandType = parameterTypes[commandIndex];
         CtClass[] newParameterTypes = Helper.prependClass(topmostInterceptor, parameterTypes);
         String visitorMethodName = visitorMethod.getName();
         CtMethod newVisitorMethod = new CtMethod(visitorMethod.getReturnType(), visitorMethodName, newParameterTypes, topmostInterceptor);
         newVisitorMethod.setModifiers(Modifier.PUBLIC | Modifier.STATIC);
         Bytecode bytecode = new Bytecode(topmostInterceptor.getClassFile().getConstPool());
         for (int i = 0; i < newParameterTypes.length; ++i) {
            bytecode.addAload(i);
         }
         String pristineMethodName = visitorMethodName + "$" + commandType.getSimpleName() + "$P";
         String descriptor = Descriptor.ofMethod(visitorMethod.getReturnType(), visitorMethod.getParameterTypes());
         bytecode.addInvokespecial(topmostInterceptor, pristineMethodName, descriptor);
         bytecode.addReturn(visitorMethod.getReturnType());
         bytecode.setMaxLocals(true, newParameterTypes, 0);
         newVisitorMethod.getMethodInfo().setCodeAttribute(bytecode.toCodeAttribute());
         topmostInterceptor.addMethod(newVisitorMethod);
      }
   }

   /**
    * Inspects call graphs and generates code for methods in the new hierarchy.
    *
    * @throws NotFoundException
    * @throws BadBytecode
    * @throws CannotCompileException
    * @throws IOException
    */
   private void createMethods() throws NotFoundException, BadBytecode, CannotCompileException, IOException {
      // Find classes that invoke the next() method in the interceptor chain
      Set<SpecificMethod> nextCallingMethods = new HashSet<>();
      for (CtClass clazz = oldInterceptor; clazz != null; clazz = clazz.getSuperclass()) {
         for (CtMethod m : clazz.getDeclaredMethods()) {
            Collection<Integer> nextInvocations = methodInvocations(m, oldInterceptor, so.nextMethodName, so.nextMethodSignature);
            if (!nextInvocations.isEmpty()) {
               nextCallingMethods.add(new SpecificMethod(m));
            }
            // TODO: so.commandClass works for CallInterceptor, but not generally
            Collection<Integer> performInvocations = methodInvocations(m, so.commandClass, so.performMethodName, so.performMethodSignature);
            if (!performInvocations.isEmpty()) {
               nextCallingMethods.add(new SpecificMethod(m));
            }
         }
      }

      Map<ProcessedMethod, NameAndDescriptor> processedMethods = new HashMap<>();
      Map<SpecificMethod, Map<Integer, SpecificMethod>> calls = new HashMap<>();
      Map<SpecificMethod, List<Caller>> invokedBy = new HashMap<>();
      Set<SpecificMethod> processed = new HashSet<>();
      for (CtMethod visitorIfaceMethod : so.visitorClass.getDeclaredMethods()) {
         String visitorMethodName = visitorIfaceMethod.getName();
         CtMethod visitorMethod = oldInterceptor.getMethod(visitorMethodName, visitorIfaceMethod.getSignature());

         // Find visitXXXCommand call graph
         findInvocations(visitorMethod, calls, invokedBy, processed);

         // Find the portion of graph that leads to next() invocation
         // Note: if the invocation goes through inner class, we've lost track of pristine/non pristine,
         // therefore we don't handle these
         Set<SpecificMethod> needsSpecialization = new HashSet<>();
         Queue<SpecificMethod> queue = new ArrayDeque<>(nextCallingMethods.stream().filter(m -> processed.contains(m)).collect(Collectors.toSet()));
         for (SpecificMethod m; (m = queue.poll()) != null; ) {
            if (!needsSpecialization.add(m)) {
               continue;
            }
            List<Caller> callers = invokedBy.get(m);
            if (callers == null) {
               continue;
            }
            for (Caller caller : callers) {
               queue.add(new SpecificMethod(caller.method));
            }
         }

         // Create a specialized copy of all the methods in the call graph
         CtClass[] parameterTypes = visitorMethod.getParameterTypes();
         int commandIndex = getCommandIndex(parameterTypes);
         if (commandIndex < 0) {
            throw new IllegalStateException("Visitor method does not contain command");
         }
         String visitorMethodType = nextInterceptor == null ? null : Descriptor.ofMethod(visitorMethod.getReturnType(), Helper.prependClass(nextInterceptor, parameterTypes));
         Set<Specialization> specializations = new HashSet<>();
         copyMethod(new SpecificMethod(visitorMethod), visitorMethodName, visitorMethodType,
               parameterTypes[commandIndex], true, calls, needsSpecialization, specializations, processedMethods);

         // Add abstract method for specializations to superclasses (as the specializations override methods there)
         for (Specialization s : specializations) {
            for (CtClass c = s.originalMethod.getDeclaringClass().getSuperclass(); c != null; c = c.getSuperclass()) {
               String name = s.originalMethod.getName();
               CtClass[] params = s.originalMethod.getParameterTypes();
               CtClass newInterceptor = newInterceptorsByOldName.get(c.getName());
               if (Helper.containsMethod(c, name, params)
                     && !Helper.containsMethod(newInterceptor, s.specializedName, s.parameterTypes)) {
                  CtMethod newMethod = new CtMethod(s.returnType, s.specializedName, s.parameterTypes, newInterceptor);
                  newMethod.setModifiers(Modifier.PROTECTED | Modifier.ABSTRACT);
                  newInterceptor.addMethod(newMethod);
               }
            }
         }
      }

      // Find complete call graph from all methods (within the class hierarchy)
      for (CtClass ctClass = oldInterceptor; ctClass != null && !ctClass.getName().equals(OBJECT_CLASS_NAME); ctClass = ctClass.getSuperclass()) {
         for (CtMethod m : ctClass.getDeclaredMethods()) {
            findInvocations(m, calls, invokedBy, processed);
         }
      }

      // Find which methods are called from visitXXX methods
      Set<SpecificMethod> inVisitorGraph = new HashSet<>();
      Queue<SpecificMethod> queue = new ArrayDeque<>(nextCallingMethods);
      for (SpecificMethod m; (m = queue.poll()) != null; ) {
         if (!inVisitorGraph.add(m)) {
            continue;
         }
         List<Caller> callers = invokedBy.get(m);
         if (callers == null) {
            continue;
         }
         for (Caller caller : callers) {
            queue.add(new SpecificMethod(caller.method));
         }
      }

      // let's not copy the non-specialized version of these methods
      for (SpecificMethod m : inVisitorGraph) {
         processedMethods.put(new ProcessedMethod(m, null, false), NameAndDescriptor.DUMMY);
      }

      // We also need to copy all non-specialized methods since these may be used from inner classes
      for (CtClass ctClass = oldInterceptor; ctClass != null && !ctClass.getName().equals(OBJECT_CLASS_NAME); ctClass = ctClass.getSuperclass()) {
         CtClass newInterceptor = newInterceptorsByOldName.get(ctClass.getName());
         for (CtMethod m : ctClass.getDeclaredMethods()) {
            // we need next method for the case that it is called from another class
            if (m.getName().equals(so.nextMethodName) && m.getSignature().equals(so.nextMethodSignature)) {
               generateNextMethod(newInterceptor, so.nextMethodName, false, null, null, null);
            } else if (newInterceptorsByOldName.containsKey(m.getReturnType().getName())) {
               // looks like a factory method or self-init method
               continue;
            } else {
               copyMethod(new SpecificMethod(m), null, null, null, false, calls, Collections.EMPTY_SET, null, processedMethods);
            }
         }
      }

      // After processing all methods, set bootstrap method tables
      for (Map.Entry<CtClass, List<BootstrapMethodsAttribute.BootstrapMethod>> entry : newBootstrapMethods.entrySet()) {
         BootstrapMethodsAttribute.BootstrapMethod[] array = entry.getValue().toArray(new BootstrapMethodsAttribute.BootstrapMethod[entry.getValue().size()]);
         CtClass newInterceptor = entry.getKey();
         newInterceptor.getClassFile().addAttribute(new BootstrapMethodsAttribute(newInterceptor.getClassFile().getConstPool(), array));
      }
   }

   /**
    * The table is used in {@link Helper#dynamicCopy(Object, Object, String[])}
    * @param baseStaticCtorCode
    */
   public void generateReplaceTable(Bytecode baseStaticCtorCode) {
      // this array should be constructed before everything as the copy constructors will require it
      baseStaticCtorCode.addIconst(replacedClassNames.size() * 2);
      baseStaticCtorCode.addAnewarray(String.class.getName());
      int counter = 0;
      for (Map.Entry<String, String> entry : replacedClassNames.entrySet()) {
         baseStaticCtorCode.addOpcode(Opcode.DUP);
         baseStaticCtorCode.addIconst(counter++);
         baseStaticCtorCode.addLdc(entry.getKey());
         baseStaticCtorCode.addOpcode(Opcode.AASTORE);
         baseStaticCtorCode.addOpcode(Opcode.DUP);
         baseStaticCtorCode.addIconst(counter++);
         baseStaticCtorCode.addLdc(entry.getValue());
         baseStaticCtorCode.addOpcode(Opcode.AASTORE);
      }
      baseStaticCtorCode.addPutstatic(baseInterceptor, REPLACE_TABLE, STRING_ARRAY_DESCRIPTOR);
   }


   public Class<?> getNewJavaInterceptor() {
      return topmostJavaInterceptor;
   }

   public CtClass getNewInterceptor() {
      return topmostInterceptor;
   }

   /**
    * Compile all generated classes into Class objects. We don't find to analyze dependency graph
    * for compilation, upon failure we retry after trying to compile all other classes.
    *
    * @throws CannotCompileException
    * @throws IOException
    */
   public void compile() throws CannotCompileException, IOException {
      Queue<CtClass> compileQueue = new ArrayDeque<>();
      Map<CtClass, Class<?>> javaClasses = new HashMap<>();
      compileQueue.addAll(newInterceptorsByOldName.values());
      compileQueue.addAll(newInnerClasses);
      int numFailures = 0;
      while (!compileQueue.isEmpty()) {
         CtClass clazz = compileQueue.poll();
         try {
            javaClasses.put(clazz, clazz.toClass());
            if (Helper.DUMP_CLASSES != null) {
               clazz.writeFile(Helper.DUMP_CLASSES);
            }
            numFailures = 0;
         } catch (CannotCompileException e) {
            compileQueue.add(clazz);
            ++numFailures;
            if (numFailures >= compileQueue.size()) {
               log.error("Cannot compile any class, in queue are: " + compileQueue);
               throw e;
            }
         }
      }
      topmostJavaInterceptor = javaClasses.get(topmostInterceptor);
   }

   private NameAndDescriptor copyMethod(SpecificMethod method, String visitorMethodName, String visitorMethodType, CtClass commandType, boolean pristine,
                                        Map<SpecificMethod, Map<Integer, SpecificMethod>> calls, Set<SpecificMethod> needsSpecialization,
                                        Set<Specialization> specializations, Map<ProcessedMethod, NameAndDescriptor> processedMethods) throws NotFoundException, BadBytecode, CannotCompileException, IOException {
      ProcessedMethod processedMethod = new ProcessedMethod(method, commandType, pristine);
      NameAndDescriptor nameAndDescriptor = processedMethods.get(processedMethod);
      if (nameAndDescriptor != null) {
         return nameAndDescriptor;
      }
      MethodInfo methodInfo = method.method.getMethodInfo();
      ConstPool oldConstPool = methodInfo.getConstPool();
      CodeAttribute codeAttribute = methodInfo.getCodeAttribute();
      // we have to deal with synchronization by synchronizing against the original interceptor
      int modifiers = method.method.getModifiers() & ~Modifier.SYNCHRONIZED;
      BootstrapMethodsAttribute bootstrapMethods = (BootstrapMethodsAttribute) method.method.getDeclaringClass().getClassFile().getAttribute(BootstrapMethodsAttribute.tag);

      CtClass[] parameterTypes = method.method.getParameterTypes();
      if (pristine) {
         int commandIndex = getCommandIndex(parameterTypes);
         parameterTypes[commandIndex] = commandType;
      }
      parameterTypes = Helper.replaceInParams(parameterTypes, replacedClassNames, classPool);

      // it's important to record processed method before recursion
      CtClass returnType = method.method.getReturnType();
      String replacedReturnType = replacedClassNames.get(returnType.getName());
      if (replacedReturnType != null) {
         returnType = classPool.get(replacedReturnType);
      }

      Simulator simulator = null;
      String newName = method.method.getName();
      if (commandType != null) {
         newName = newName + "$" + commandType.getSimpleName() + (pristine ? "$P" : "$o");
         specializations.add(new Specialization(method.method, newName, parameterTypes, returnType));
         if (pristine && codeAttribute != null) {
            simulator = new Simulator();
            try {
               simulator.run(Modifier.isStatic(modifiers), getCommandIndex(method), oldConstPool, codeAttribute);
            } catch (Exception e) {
               throw new RuntimeException("Failure simulating method " + method.method.getLongName(), e);
            }
         }
      }

      CtClass newInterceptor = newInterceptorsByOldName.get(method.method.getDeclaringClass().getName());
      ConstPool newConstPool = newInterceptor.getClassFile().getConstPool();
      String descriptor = Descriptor.ofMethod(returnType, parameterTypes);
      nameAndDescriptor = new NameAndDescriptor(newInterceptor.getName(), newName, descriptor);
      processedMethods.put(processedMethod, nameAndDescriptor);

      CtMethod newMethod = new CtMethod(returnType, newName, parameterTypes, newInterceptor);
      newMethod.setModifiers(modifiers);
      newMethod.setExceptionTypes(method.method.getExceptionTypes());
      MethodInfo newMethodInfo = newMethod.getMethodInfo();
      AnnotationsAttribute annotations = (AnnotationsAttribute) methodInfo.getAttribute(AnnotationsAttribute.visibleTag);
      if (annotations != null) {
         newMethodInfo.getAttributes().add(annotations.copy(newConstPool, replacedSlashedNames));
      }
      ParameterAnnotationsAttribute parameterAnnotations = (ParameterAnnotationsAttribute) methodInfo.getAttribute(ParameterAnnotationsAttribute.visibleTag);
      if (parameterAnnotations != null) {
         newMethodInfo.getAttributes().add(parameterAnnotations.copy(newConstPool, replacedClassNames));
      }
      newInterceptor.addMethod(newMethod);

      // Abstract methods need to be copied, too
      if (codeAttribute != null) {
         CodeAttribute newCodeAttribute = createNewCode(codeAttribute, newConstPool);
         newCodeAttribute.getAttributes().add(codeAttribute.getAttribute(LineNumberAttribute.tag).copy(newConstPool, null));
         newCodeAttribute.getAttributes().add(codeAttribute.getAttribute(LocalVariableAttribute.tag).copy(newConstPool, null));

         StackMapTable stackMapTable = (StackMapTable) codeAttribute.getAttribute(StackMapTable.tag);
         if (stackMapTable != null) {
            int initialLocals = Descriptor.paramSize(descriptor) + (Modifier.isStatic(modifiers) ? 0 : 1);
            newCodeAttribute.getAttributes().add(copyStackMapTable(initialLocals, oldConstPool, newConstPool, stackMapTable, pristine ? commandType : null, simulator));
         }

         // Iterator will shift all other attributes properly, so all attributes must be set at this point
         adjustCode(method, visitorMethodName, visitorMethodType, commandType, pristine, simulator,
               newInterceptor, oldConstPool, newConstPool, bootstrapMethods, codeAttribute, newCodeAttribute,
               calls, needsSpecialization, processedMethods, specializations);

         if (Modifier.isSynchronized(method.method.getModifiers())) {
            CodeAttribute syncCodeAttribute = generateSynchronizationMethod(method, newInterceptor, newConstPool, newName, parameterTypes, returnType, newCodeAttribute);
            newMethodInfo.addAttribute(syncCodeAttribute);
         } else {
            newMethodInfo.addAttribute(newCodeAttribute);
         }
      }
      return nameAndDescriptor;
   }

   /**
    * As outer code (most likely in tests) can synchronize on the interceptor itself,
    * cloned methods of originally synchronized methods must sync on the same object.
    * Therefore, we'll replace the actual method code with wrapper code calling monitorenter
    * and monitorexit on the original interceptor, and hide the actual code of method
    * into new method with $sync suffix.
    *
    * @param method
    * @param newInterceptor
    * @param newConstPool
    * @param newName
    * @param parameterTypes
    * @param returnType
    * @param newCodeAttribute
    * @return
    * @throws NotFoundException
    * @throws CannotCompileException
    */
   private CodeAttribute generateSynchronizationMethod(SpecificMethod method, CtClass newInterceptor, ConstPool newConstPool,
          String newName, CtClass[] parameterTypes, CtClass returnType, CodeAttribute newCodeAttribute) throws NotFoundException, CannotCompileException {
      CtMethod syncMethod = new CtMethod(returnType, newName + SYNC_SUFFIX, parameterTypes, newInterceptor);
      syncMethod.setModifiers(method.method.getModifiers());
      syncMethod.setExceptionTypes(method.method.getExceptionTypes());
      syncMethod.getMethodInfo().addAttribute(newCodeAttribute);
      newInterceptor.addMethod(syncMethod);

      Bytecode syncBytecode = new Bytecode(newConstPool);
      syncBytecode.addAload(0);
      syncBytecode.addGetfield(newInterceptor, SYNC_ROOT, Descriptor.of(oldInterceptor));
      syncBytecode.addOpcode(Opcode.DUP);
      syncBytecode.addOpcode(Opcode.MONITORENTER);
      int syncBlockBegin = syncBytecode.length();
      syncBytecode.addAload(0);
      syncBytecode.addLoadParameters(parameterTypes, 1);
      syncBytecode.addInvokespecial(newInterceptor, newName + SYNC_SUFFIX, returnType, parameterTypes);
      if (!returnType.equals(CtClass.voidType)) {
         syncBytecode.addOpcode(Opcode.SWAP);
      }
      syncBytecode.addOpcode(Opcode.MONITOREXIT);
      int syncBlockEnd = syncBytecode.length();
      syncBytecode.addReturn(returnType);
      int handler = syncBytecode.length();
      syncBytecode.addAload(0);
      syncBytecode.addGetfield(newInterceptor, SYNC_ROOT, Descriptor.of(oldInterceptor));
      syncBytecode.addOpcode(Opcode.MONITOREXIT);
      syncBytecode.addOpcode(Opcode.ATHROW);
      syncBytecode.addExceptionHandler(syncBlockBegin, syncBlockEnd, handler, "java/lang/Throwable");
      syncBytecode.setMaxLocals(false, parameterTypes, 0);

      StackMapTable.Writer stackMapWriter = new StackMapTable.Writer(16);
      int[] localTags = new int[parameterTypes.length + 1];
      int[] localData = new int[parameterTypes.length + 1];
      localTags[0] = StackMapTable.OBJECT;
      localData[0] = newConstPool.getThisClassInfo();
      for (int i = 0; i < parameterTypes.length; ++i) {
         localTags[i + 1] = parameterTypes[i].isPrimitive() ? StackMapTable.typeTagOf(((CtPrimitiveType) parameterTypes[i]).getDescriptor()) : StackMapTable.OBJECT;
         localData[i + 1] = newConstPool.addClassInfo(parameterTypes[i]);
      }
      int stackTags[] = new int[] { StackMapTable.OBJECT };
      int stackData[] = new int[] { newConstPool.addClassInfo(Throwable.class.getName()) };
      stackMapWriter.fullFrame(handler, localTags, localData, stackTags, stackData);
      CodeAttribute syncCodeAttribute = syncBytecode.toCodeAttribute();
      syncCodeAttribute.getAttributes().add(stackMapWriter.toStackMapTable(newConstPool));
      return syncCodeAttribute;
   }

   private StackMapTable copyStackMapTable(int initialLocals, final ConstPool oldConstPool, ConstPool newConstPool, StackMapTable stackMapTable, CtClass commandType, Simulator simulator) throws BadBytecode {
      final StackMapTable.Writer writer = new StackMapTable.Writer(stackMapTable.length());;
      StackMapTable.Walker walker = new StackMapTable.Walker(stackMapTable) {
         private int offset = 0;
         private int locals = initialLocals;

         @Override
         public void sameFrame(int pos, int offsetDelta) throws BadBytecode {
            offset += offsetDelta + (offset == 0 ? 0 : 1);
            writer.sameFrame(offsetDelta);
         }

         @Override
         public void sameLocals(int pos, int offsetDelta, int stackTag, int stackData) throws BadBytecode {
            offset += offsetDelta + (offset == 0 ? 0 : 1);
            stackData = convert(stackTag, stackData, true, 0);
            writer.sameLocals(offsetDelta, stackTag, stackData);
         }

         @Override
         public void chopFrame(int pos, int offsetDelta, int k) throws BadBytecode {
            offset += offsetDelta + (offset == 0 ? 0 : 1);
            locals -= k;
            writer.chopFrame(offsetDelta, k);
         }

         @Override
         public void appendFrame(int pos, int offsetDelta, int[] tags, int[] data) throws BadBytecode {
            offset += offsetDelta + (offset == 0 ? 0 : 1);
            convert(tags, data, false);
            locals += tags.length;
            writer.appendFrame(offsetDelta, tags, data);
         }

         @Override
         public void fullFrame(int pos, int offsetDelta, int[] localTags, int[] localData, int[] stackTags, int[] stackData) throws BadBytecode {
            offset += offsetDelta + (offset == 0 ? 0 : 1);
            locals = 0;
            convert(localTags, localData, false);
            convert(stackTags, stackData, true);
            locals = localTags.length;
            writer.fullFrame(offsetDelta, localTags, localData, stackTags, stackData);
         }

         public void convert(int[] tags, int[] data, boolean stack) {
            for (int i = 0; i < tags.length; ++i) {
               data[i] = convert(tags[i], data[i], stack, locals + i);
            }
         }

         public int convert(int tag, int data, boolean stack, int pos) {
            if (tag == StackMapTable.OBJECT) {
               String className = oldConstPool.getClassInfo(data);
               String replaced;
               try {
                  if ((replaced = replacedClassNames.get(className)) != null) {
                     return newConstPool.addClassInfo(replaced);
                  } else {
                     CtClass dataClass = classPool.get(className);
                     if (commandType != null && !dataClass.subtypeOf(commandType) && dataClass.subtypeOf(so.commandClass)
                           && ((stack && simulator.checkStack(offset, pos)) ||
                           (!stack && simulator.checkLocals(offset, pos)))) {
                        return newConstPool.addClassInfo(commandType);
                     } else {
                        return newConstPool.addClassInfo(className);
                     }
                  }
               } catch (NotFoundException e) {
                  throw new RuntimeException(e);
               }
            }
            return data;
         }
      };
      walker.parse();
      return writer.toStackMapTable(newConstPool);
   }

   private CodeAttribute adjustCode(SpecificMethod method, String visitorMethodName, String visitorMethodType, CtClass commandType, boolean pristine,
                                    Simulator simulator, CtClass newInterceptor, ConstPool oldConstPool,
                                    ConstPool newConstPool, BootstrapMethodsAttribute bootstrapMethods, CodeAttribute codeAttribute,
                                    CodeAttribute newCodeAttribute, Map<SpecificMethod, Map<Integer, SpecificMethod>> calls,
                                    Set<SpecificMethod> needsSpecialization,
                                    Map<ProcessedMethod, NameAndDescriptor> processedMethods, Set<Specialization> specializations) throws BadBytecode, NotFoundException, CannotCompileException, IOException {

      Map<Integer, SpecificMethod> methodCalls = calls.get(method);
      CodeIterator iterator = codeAttribute.iterator();
      CodeIterator newIterator = newCodeAttribute.iterator();
      newIterator.begin();

      int codeLength = iterator.getCodeLength();
      while (iterator.hasNext()) {
         int readPc = iterator.next();
         // due to changed instructions (wide versions) the PCs may diverge
         int writePc = newIterator.next();
         int op = iterator.byteAt(readPc);
         int arg8 = readPc < codeLength - 1 ? iterator.byteAt(readPc + 1) : 0;
         int arg16 = readPc < codeLength - 2 ? iterator.u16bitAt(readPc + 1) : 0;
         int ref, classInfo;
         String className, fieldName, methodName, type, replaced;
         switch (op) {
            case Opcode.CHECKCAST:
               className = oldConstPool.getClassInfo(arg16);
               replaced = replacedClassNames.get(className);
               if (replaced != null) {
                  newIterator.write16bit(newConstPool.addClassInfo(replaced), writePc + 1);
               } else {
                  CtClass castTo = classPool.get(className);
                  if (simulator != null && castTo.subtypeOf(so.commandClass) && !castTo.subtypeOf(commandType) && simulator.checkStack(readPc, 0)) {
                     newIterator.write16bit(newConstPool.addClassInfo(commandType), writePc + 1);
                  } else {
                     newIterator.write16bit(newConstPool.addClassInfo(className), writePc + 1);
                  }
               }
               break;
            case Opcode.ANEWARRAY:
            case Opcode.INSTANCEOF:
            case Opcode.MULTIANEWARRAY:
            case Opcode.NEW:
               className = oldConstPool.getClassInfo(arg16);
               replaced = replacedClassNames.get(className);
               if (replaced != null) {
                  className = replaced;
               }
               newIterator.write16bit(newConstPool.addClassInfo(className), writePc + 1);
               break;
            case Opcode.GETFIELD:
            case Opcode.GETSTATIC:
            case Opcode.PUTFIELD:
            case Opcode.PUTSTATIC:
               className = oldConstPool.getFieldrefClassName(arg16);
               fieldName = oldConstPool.getFieldrefName(arg16);
               if ((replaced = replacedClassNames.get(className)) != null){
                  if (className.equals(so.nextFieldClass.getName()) && fieldName.equals(so.nextFieldName)) {
                     if (op == Opcode.GETFIELD) {
                        newIterator.writeByte(Opcode.POP, writePc); // reference
                        newIterator.writeByte(Opcode.ACONST_NULL, writePc + 1);
                        newIterator.writeByte(Opcode.NOP, writePc + 2);
                     } else if (op == Opcode.PUTFIELD) {
                        newIterator.writeByte(Opcode.POP, writePc);
                        newIterator.writeByte(Opcode.POP, writePc + 1);
                        newIterator.writeByte(Opcode.NOP, writePc + 2);
                     } else {
                        throw new IllegalStateException();
                     }
                     break;
                  } else {
                     classInfo = newConstPool.addClassInfo(replaced);
                  }
               } else {
                  classInfo = newConstPool.addClassInfo(className);
               }
               type = oldConstPool.getFieldrefType(arg16);
               if ((replaced = replacedClassNames.get(Descriptor.toClassName(type))) != null) {
                  type = Descriptor.of(replaced);
               }
               ref = newConstPool.addFieldrefInfo(classInfo, fieldName, type);
               newIterator.write16bit(ref, writePc + 1);
               break;
            case Opcode.INVOKEDYNAMIC:
               int bootstrap = oldConstPool.getInvokeDynamicBootstrap(arg16);
               int bootstrapNameAndType = oldConstPool.getInvokeDynamicNameAndType(arg16);
               int bootstrapName = oldConstPool.getNameAndTypeName(bootstrapNameAndType);
               int bootstrapType = oldConstPool.getNameAndTypeDescriptor(bootstrapNameAndType);
               int newBootstrapNameAndType = newConstPool.addNameAndTypeInfo(oldConstPool.getUtf8Info(bootstrapName), Helper.replaceInDescriptor(classPool, replacedClassNames, oldConstPool.getUtf8Info(bootstrapType)));
               BootstrapMethodsAttribute.BootstrapMethod bootstrapMethod = bootstrapMethods.getMethods()[bootstrap];
               Helper.MethodCopier methodCopier = m -> {
                  try {
                     return copyMethod(new SpecificMethod(m), null, null, null, false, calls, needsSpecialization, specializations, processedMethods);
                  } catch (Exception e) {
                     throw new RuntimeException(e);
                  }
               };
               int newBootstrapMethodHandle = Helper.copyMethodHandle(classPool, oldConstPool, newConstPool, bootstrapMethod.methodRef, replacedClassNames, methodCopier);
               int[] newBootstrapMethodArguments = Helper.copyBootstrapMethodArguments(classPool, oldConstPool, newConstPool, bootstrapMethod, replacedClassNames, methodCopier);
               List<BootstrapMethodsAttribute.BootstrapMethod> newBootstrapMethods = this.newBootstrapMethods.get(newInterceptor);
               if (newBootstrapMethods == null) {
                  this.newBootstrapMethods.put(newInterceptor, newBootstrapMethods = new ArrayList<>());
               }
               int newBootstrapIndex = newBootstrapMethods.size();
               newBootstrapMethods.add(new BootstrapMethodsAttribute.BootstrapMethod(newBootstrapMethodHandle, newBootstrapMethodArguments));
               int newInvokeDynamic = newConstPool.addInvokeDynamicInfo(newBootstrapIndex, newBootstrapNameAndType);
               newIterator.write16bit(newInvokeDynamic, writePc + 1);
               break;
            case Opcode.INVOKEINTERFACE:
               className = oldConstPool.getInterfaceMethodrefClassName(arg16);
               methodName = oldConstPool.getInterfaceMethodrefName(arg16);
               type = oldConstPool.getInterfaceMethodrefType(arg16);
               ref = newConstPool.addInterfaceMethodrefInfo(newConstPool.addClassInfo(className), methodName, type);
               newIterator.write16bit(ref, writePc + 1);
               break;
            case Opcode.INVOKEVIRTUAL:
            case Opcode.INVOKESPECIAL:
            case Opcode.INVOKESTATIC:
               SpecificMethod invoked = methodCalls != null ? methodCalls.get(readPc) : null;
               if (invoked == null) {
                  // invokestatic can reference interface methods, too
                  int tag = oldConstPool.getTag(arg16);
                  if (tag == Constants.METHOD_REF_INFO) {
                     className = oldConstPool.getMethodrefClassName(arg16);
                     type = oldConstPool.getMethodrefType(arg16);
                     methodName = oldConstPool.getMethodrefName(arg16);
                  } else if (tag == Constants.IFACE_METHOD_REF_INFO) {
                     className = oldConstPool.getInterfaceMethodrefClassName(arg16);
                     type = oldConstPool.getInterfaceMethodrefType(arg16);
                     methodName = oldConstPool.getInterfaceMethodrefName(arg16);
                  } else {
                     throw new IllegalArgumentException("Unexpected: " + tag);
                  }

                  replaced = replacedClassNames.get(className);
                  if (replaced != null) {
                     className = replaced;
                     type = Helper.replaceInDescriptor(classPool, replacedClassNames, type);
                  } else if (op == Opcode.INVOKEVIRTUAL) {
                     CtClass invokedClass = classPool.get(className);
                     CtMethod invokedMethod = invokedClass.getMethod(methodName, type);
                     if (commandType != null && !commandType.subtypeOf(invokedClass) && !invokedClass.subtypeOf(commandType)
                           && simulator != null && simulator.checkStack(readPc, invokedMethod.getParameterTypes().length)) {
                        NameAndDescriptor sink = generateSinkMethod(newInterceptor, newConstPool, commandType, invokedClass.getMethod(methodName, type));
                        ref = newConstPool.addMethodrefInfo(newConstPool.getThisClassInfo(), sink.getName(), sink.getDescriptor());
                        newIterator.writeByte(Opcode.INVOKESTATIC, writePc);
                        newIterator.write16bit(ref, writePc + 1);
                        break;
                     }
                  }
                  if (tag == Constants.METHOD_REF_INFO) {
                     ref = newConstPool.addMethodrefInfo(newConstPool.addClassInfo(className), methodName, type);
                  } else {
                     ref = newConstPool.addInterfaceMethodrefInfo(newConstPool.addClassInfo(className), methodName, type);
                  }
               } else if (invoked.method.getName().equals(so.nextMethodName) && invoked.method.getSignature().equals(so.nextMethodSignature)) {
                  int stackDepth = invoked.method.getParameterTypes().length - getCommandIndex(invoked) - 1;
                  boolean callPristine = pristine ? simulator.checkStack(readPc, stackDepth) : false;
                  String newNextMethodName = so.nextMethodName + (commandType != null ? "$" + commandType.getSimpleName() : "")+ (callPristine ? "$P" : "$O");
                  String nextSignature = generateNextMethod(nextMethodInterceptor, newNextMethodName, callPristine, commandType, visitorMethodName, visitorMethodType);
                  ref = newConstPool.addMethodrefInfo(newConstPool.addClassInfo(nextMethodInterceptor), newNextMethodName, nextSignature);
                  newIterator.writeByte(Opcode.INVOKEVIRTUAL, writePc);
               } else {
                  String newClassName = replacedClassNames.get(oldConstPool.getMethodrefClassName(arg16));
                  if (needsSpecialization.contains(invoked)) {
                     int stackDepth = invoked.method.getParameterTypes().length - getCommandIndex(invoked) - 1;
                     boolean callPristine = pristine ? simulator.checkStack(readPc, stackDepth) : false;
                     NameAndDescriptor copied = copyMethod(invoked, visitorMethodName, visitorMethodType, commandType, callPristine, calls, needsSpecialization, specializations, processedMethods);
                     ref = newConstPool.addMethodrefInfo(newConstPool.addClassInfo(newClassName), copied.getName(), copied.getDescriptor());
                  } else {
                     NameAndDescriptor copied = copyMethod(invoked, visitorMethodName, visitorMethodType, null, false, calls, needsSpecialization, specializations, processedMethods);
                     ref = newConstPool.addMethodrefInfo(newConstPool.addClassInfo(newClassName), copied.getName(), copied.getDescriptor());
                  }
               }
               newIterator.write16bit(ref, writePc + 1);
               break;
            case Opcode.LDC:
               ref = getSingleWordInfo(newConstPool, oldConstPool, arg8);
               if (ref <= 255) {
                  newIterator.writeByte(ref, writePc + 1);
               } else {
                  newIterator.insertGapAt(writePc, 1, false);
                  newIterator.writeByte(Opcode.LDC_W, writePc);
                  newIterator.write16bit(ref, writePc + 1);
               }
               break;
            case Opcode.LDC_W:
               ref = getSingleWordInfo(newConstPool, oldConstPool, arg16);
               newIterator.write16bit(ref, writePc + 1);
               break;
            case Opcode.LDC2_W:
               ref = getDoubleWordInfo(newConstPool, oldConstPool, arg16);
               newIterator.write16bit(ref, writePc + 1);
               break;
         }
      }
      return newCodeAttribute;
   }

   /**
    * This method is used as a replacement for unreachable method invocation, consuming the arguments
    * properly so that stack map table is consistent. It should be never called, an in case it is
    * (bug in this class) it throws {@link UnsupportedOperationException}
    *
    * @param newInterceptor
    * @param newConstPool
    * @param commandType
    * @param method
    * @return
    * @throws NotFoundException
    * @throws CannotCompileException
    */
   private NameAndDescriptor generateSinkMethod(CtClass newInterceptor, ConstPool newConstPool, CtClass commandType, CtMethod method) throws NotFoundException, CannotCompileException {
      CtMethod sinkMethod = new CtMethod(method.getReturnType(), "$sink" + sinkMethodCounter++,
            Helper.prependClass(commandType, method.getParameterTypes()), newInterceptor);
      sinkMethod.setModifiers(Modifier.PRIVATE | Modifier.STATIC | AccessFlag.SYNTHETIC);
      Bytecode bytecode = new Bytecode(newConstPool, 2, method.getParameterTypes().length + 1);
      int unsupportedOperationException = newConstPool.addClassInfo(UnsupportedOperationException.class.getName());
      bytecode.addOpcode(Opcode.NEW);
      bytecode.addIndex(unsupportedOperationException);
      bytecode.addOpcode(Opcode.DUP);
      bytecode.addInvokespecial(unsupportedOperationException, "<init>", "()V");
      bytecode.addOpcode(Opcode.ATHROW);
      CodeAttribute codeAttribute = bytecode.toCodeAttribute();
      codeAttribute.getAttributes().add(new StackMapTable.Writer(0).toStackMapTable(newConstPool));
      sinkMethod.getMethodInfo().setCodeAttribute(codeAttribute);
      newInterceptor.addMethod(sinkMethod);
      return new NameAndDescriptor(newInterceptor.getName(), sinkMethod.getName(), sinkMethod.getSignature());
   }


   private CodeAttribute createNewCode(CodeAttribute codeAttribute, ConstPool newConstPool) {
      byte[] newBytecode = Arrays.copyOf(codeAttribute.getCode(), codeAttribute.getCodeLength());
      return new CodeAttribute(newConstPool, codeAttribute.getMaxStack(), codeAttribute.getMaxLocals(),
            newBytecode, codeAttribute.getExceptionTable().copy(newConstPool, null));
   }

   /**
    * Generate a method that will call next interceptor or command.perform()
    *
    * @param newInterceptor
    * @param newName
    * @param pristine
    * @param commandType
    * @param visitorMethodName
    * @param visitorMethodType
    * @return
    * @throws NotFoundException
    * @throws CannotCompileException
    */
   private String generateNextMethod(CtClass newInterceptor, String newName, boolean pristine, CtClass commandType, String visitorMethodName, String visitorMethodType) throws NotFoundException, CannotCompileException {
      // check if we have already processed this method
      String signature = syntheticNextMethods.get(newName);
      if (signature != null) {
         return signature;
      }
      CtClass returnType = Descriptor.getReturnType(so.nextMethodSignature, classPool);
      CtClass[] parameterTypes = Descriptor.getParameterTypes(so.nextMethodSignature, classPool);
      if (pristine) {
         int commandIndex = getCommandIndex(parameterTypes);
         parameterTypes[commandIndex] = commandType;
      }
      CtMethod newMethod = new CtMethod(returnType, newName, parameterTypes, newInterceptor);
      newMethod.setModifiers(Modifier.PUBLIC | Modifier.FINAL);
      MethodInfo newMethodInfo = newMethod.getMethodInfo();
      newInterceptor.addMethod(newMethod);

      ConstPool newConstPool = newInterceptor.getClassFile().getConstPool();
      Bytecode bytecode = new Bytecode(newConstPool);
      if (nextInterceptor != null) {
         if (pristine) {
            // TODO: if the next() and visit() params don't match, adapt it here
            bytecode.addAload(0);
            bytecode.addGetfield(newInterceptor, newNextField.getName(), Descriptor.of(nextInterceptor));
            bytecode.addLoadParameters(parameterTypes, 1);
            bytecode.addInvokestatic(nextInterceptor, visitorMethodName, visitorMethodType);
         } else {
            // TODO: in some non-pristine calls we can still guess the type from StackMapTable
            generateInvoke(newInterceptor, bytecode, so.commandClass, so.acceptMethodName, so.acceptMethodSignature, so.acceptArgs);
         }
      } else {
         // Static version of perform not needed, compiler can inline this correctly
         generateInvoke(newInterceptor, bytecode, so.commandClass, so.performMethodName, so.performMethodSignature, so.performArgs);
      }
      bytecode.addReturn(returnType);
      bytecode.setMaxLocals(false, parameterTypes, 0);

      CodeAttribute codeAttribute = bytecode.toCodeAttribute();
      StackMapTable.Writer writer = new StackMapTable.Writer(2);
      codeAttribute.setAttribute(writer.toStackMapTable(newConstPool));
      newMethodInfo.addAttribute(codeAttribute);

      signature = Descriptor.ofMethod(returnType, parameterTypes);
      syntheticNextMethods.put(newName, signature);
      return signature;
   }

   /**
    * Generate invocation with arguments from given local variables; -1 stands for the next delegator.
    *
    * @param newInterceptor
    * @param bytecode
    * @param commandClass
    * @param methodName
    * @param methodSignature
    * @param args
    */
   private void generateInvoke(CtClass newInterceptor, Bytecode bytecode, CtClass commandClass, String methodName, String methodSignature, int[] args) {
      for (int i = 0; i < args.length; ++i) {
         // TODO: the argument might be primitive
         if (args[i] >= 0) {
            bytecode.addAload(args[i]);
         } else {
            bytecode.addNew(nextDelegator);
            bytecode.addOpcode(Opcode.DUP);
            bytecode.addAload(0);
            bytecode.addGetfield(newInterceptor, newNextField.getName(), Descriptor.of(nextInterceptor));
            bytecode.addInvokespecial(nextDelegator, "<init>", CtClass.voidType, new CtClass[] {nextInterceptor});
         }
      }
      if (commandClass.isInterface()) {
         // objectreference is not included in count, therefore -1
         bytecode.addInvokeinterface(commandClass, methodName, methodSignature, args.length);
      } else {
         bytecode.addInvokevirtual(commandClass, methodName, methodSignature);
      }
   }

   private int getDoubleWordInfo(ConstPool newConstPool, ConstPool oldConstPool, int index) {
      switch (oldConstPool.getTag(index)) {
         case Constants.LONG_INFO:
            return newConstPool.addLongInfo(oldConstPool.getLongInfo(index));
         case Constants.DOUBLE_INFO:
            return newConstPool.addDoubleInfo(oldConstPool.getDoubleInfo(index));
         default:
            throw new IllegalArgumentException("Unexpected tag: " + oldConstPool.getTag(index));
      }
   }

   private int getSingleWordInfo(ConstPool newConstPool, ConstPool oldConstPool, int index) {
      switch (oldConstPool.getTag(index)) {
         case Constants.INTEGER_INFO:
            return newConstPool.addIntegerInfo(oldConstPool.getIntegerInfo(index));
         case Constants.FLOAT_INFO:
            return newConstPool.addFloatInfo(oldConstPool.getFloatInfo(index));
         case Constants.STRING_INFO:
            return newConstPool.addStringInfo(oldConstPool.getStringInfo(index));
         case Constants.CLASS_INFO:
            return newConstPool.addClassInfo(oldConstPool.getClassInfo(index));
         default:
            throw new IllegalArgumentException("Unexpected tag: " + oldConstPool.getTag(index));
      }
   }

   private int getCommandIndex(SpecificMethod method) throws NotFoundException {
      CtClass[] parameterTypes = method.method.getParameterTypes();
      return getCommandIndex(parameterTypes);
   }

   private int getCommandIndex(CtClass[] parameterTypes) throws NotFoundException {
      int commandIndex = -1;
      int i = 0;
      for (CtClass param : parameterTypes) {
         if (param.subtypeOf(so.commandClass)) {
            if (commandIndex >= 0) {
               throw new IllegalStateException("Two commands as methods arguments?");
            }
            commandIndex = i;
         }
         ++i;
      }
      return commandIndex;
   }

   /**
    * Find the call graph of certain method.
    *
    * @param caller
    * @param methodCalls
    * @param invokedBy
    * @param processed
    * @throws BadBytecode
    * @throws NotFoundException
    */
   private void findInvocations(CtMethod caller,
                                Map<SpecificMethod, Map<Integer, SpecificMethod>> methodCalls,
                                Map<SpecificMethod, List<Caller>> invokedBy,
                                Set<SpecificMethod> processed) throws BadBytecode, NotFoundException {
      MethodInfo callerInfo = caller.getMethodInfo();
      SpecificMethod callerMethod = new SpecificMethod(caller);
      if (!processed.add(callerMethod)) {
         return;
      }

      CodeAttribute codeAttribute = callerInfo.getCodeAttribute();
      if (codeAttribute == null) {
         return;
      }
      ConstPool constPool = callerInfo.getConstPool();
      CodeIterator iterator = codeAttribute.iterator();

      while (iterator.hasNext()) {
         int pc = iterator.next();
         int op = iterator.byteAt(pc);
         switch (op) {
            case Opcode.INVOKEVIRTUAL:
            case Opcode.INVOKESPECIAL:
            case Opcode.INVOKESTATIC:
               int invokedRef = iterator.u16bitAt(pc + 1);
               String invocationClass = constPool.getMethodrefClassName(invokedRef);
               if (OBJECT_CLASS_NAME.equals(invocationClass)) {
                  continue;
               }
               CtClass ctClass = classPool.get(invocationClass);
               String invokedMethodName = constPool.getMethodrefName(invokedRef);
               String invokedMethodType = constPool.getMethodrefType(invokedRef);
               if (oldInterceptor.subclassOf(ctClass)) {
                  CtMethod invoked = null;
                  if (op == Opcode.INVOKEVIRTUAL) {
                     invoked = oldInterceptor.getMethod(invokedMethodName, invokedMethodType);
                  } else {
                     // super.foo() may target super.super.foo(), but still use just super in method ref class
                     while (invoked == null && ctClass != null) {
                        for (CtMethod m : ctClass.getDeclaredMethods()) {
                           if (m.getName().equals(invokedMethodName) && m.getSignature().equals(invokedMethodType)) {
                              invoked = m;
                              break;
                           }
                        }
                        ctClass = ctClass.getSuperclass();
                     }
                  }
                  if (invoked == null) {
                     if ("<init>".equals(invokedMethodName) && Modifier.isStatic(caller.getModifiers())) {
                        // constructor called from helper static method - ignore
                        continue;
                     } else {
                        throw new IllegalStateException(Mnemonic.OPCODE[op] + " " + invocationClass + "." + invokedMethodName + invokedMethodType);
                     }
                  }
                  SpecificMethod invokedMethod = new SpecificMethod(invoked);
                  List<Caller> callers = invokedBy.get(invokedMethod);
                  if (callers == null) {
                     invokedBy.put(invokedMethod, callers = new ArrayList<>());
                  }
                  callers.add(new Caller(caller, pc));
                  Map<Integer, SpecificMethod> callMap = methodCalls.get(callerMethod);
                  if (callMap == null) {
                     methodCalls.put(callerMethod, callMap = new HashMap<>());
                  }
                  callMap.put(pc, invokedMethod);
                  findInvocations(invoked, methodCalls, invokedBy, processed);
               }
               break;
         }
      }
   }

   /**
    * Get a list of positions in bytecode where given method is invoked
    *
    * @param m Method to search in
    * @param invokedSubtype Target class of invocation (or one of it's subclasses)
    * @param invokedName Name of the method
    * @param invokedSignature Signature of the method
    * @return
    * @throws BadBytecode
    * @throws NotFoundException
    */
   private Collection<Integer> methodInvocations(CtMethod m, CtClass invokedSubtype, String invokedName, String invokedSignature) throws BadBytecode, NotFoundException {
      MethodInfo methodInfo = m.getMethodInfo();
      CodeAttribute codeAttribute = methodInfo.getCodeAttribute();
      if (codeAttribute == null) {
         assert Modifier.isNative(m.getModifiers()) || Modifier.isAbstract(m.getModifiers());
         return Collections.EMPTY_LIST;
      }
      ConstPool constPool = methodInfo.getConstPool();
      ArrayList<Integer> invocations = new ArrayList<>();
      CodeIterator iterator = codeAttribute.iterator();
      while (iterator.hasNext()) {
         int index = iterator.next();
         int op = iterator.byteAt(index);
         switch (op) {
         case Opcode.INVOKEVIRTUAL:
            int methodRef = iterator.u16bitAt(index + 1);
            CtClass methodClass = classPool.get(constPool.getMethodrefClassName(methodRef));
            if (invokedSubtype.subtypeOf(methodClass)
                  && constPool.getMethodrefName(methodRef).equals(invokedName)
                  && constPool.getMethodrefType(methodRef).equals(invokedSignature)) {
               invocations.add(index);
            }
            break;
         case Opcode.INVOKEINTERFACE:
            int ifaceRef = iterator.u16bitAt(index + 1);
            CtClass ifaceClass = classPool.get(constPool.getInterfaceMethodrefClassName(ifaceRef));
            if (invokedSubtype.subtypeOf(ifaceClass)
                  && constPool.getInterfaceMethodrefName(ifaceRef).equals(invokedName)
                  && constPool.getInterfaceMethodrefType(ifaceRef).equals(invokedSignature)) {
               invocations.add(index);
            }
            break;
         }
      }
      return invocations;
   }

   private static class ProcessedMethod {
      public final SpecificMethod originalMethod;
      public final CtClass commandType;
      public final boolean pristine;

      public ProcessedMethod(SpecificMethod originalMethod, CtClass commandType, boolean pristine) {
         this.originalMethod = originalMethod;
         this.commandType = commandType;
         this.pristine = pristine;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         ProcessedMethod that = (ProcessedMethod) o;

         if (pristine != that.pristine) return false;
         if (!originalMethod.equals(that.originalMethod)) return false;
         return !(commandType != null ? !commandType.equals(that.commandType) : that.commandType != null);

      }

      @Override
      public int hashCode() {
         int result = originalMethod.hashCode();
         result = 31 * result + (commandType != null ? commandType.hashCode() : 0);
         result = 31 * result + (pristine ? 1 : 0);
         return result;
      }
   }

   private static class Inner {
      private final CtClass oldClass;
      private final CtClass newClass;

      public Inner(CtClass oldClass, CtClass newClass) {
         this.oldClass = oldClass;
         this.newClass = newClass;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         Inner inner = (Inner) o;

         if (!oldClass.equals(inner.oldClass)) return false;
         return newClass.equals(inner.newClass);

      }

      @Override
      public int hashCode() {
         int result = oldClass.hashCode();
         result = 31 * result + newClass.hashCode();
         return result;
      }
   }

   private static class Specialization {
      private final CtMethod originalMethod;
      private final String specializedName;
      private final CtClass[] parameterTypes;
      private final CtClass returnType;

      public Specialization(CtMethod originalMethod, String specializedName, CtClass[] parameterTypes, CtClass returnType) {
         this.originalMethod = originalMethod;
         this.specializedName = specializedName;
         this.parameterTypes = parameterTypes;
         this.returnType = returnType;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         Specialization that = (Specialization) o;

         if (!originalMethod.equals(that.originalMethod)) return false;
         if (!specializedName.equals(that.specializedName)) return false;
         // Probably incorrect - comparing Object[] arrays with Arrays.equals
         if (!Arrays.equals(parameterTypes, that.parameterTypes)) return false;
         return returnType.equals(that.returnType);

      }

      @Override
      public int hashCode() {
         int result = originalMethod.hashCode();
         result = 31 * result + specializedName.hashCode();
         result = 31 * result + Arrays.hashCode(parameterTypes);
         result = 31 * result + returnType.hashCode();
         return result;
      }
   }
}

