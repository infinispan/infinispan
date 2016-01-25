package org.infinispan.stack;

import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtConstructor;
import javassist.CtField;
import javassist.CtMethod;
import javassist.Modifier;
import javassist.NotFoundException;
import javassist.bytecode.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Generates hierarchy of interceptors' nested classes, which often keep a reference
 * to the interceptor which needs to be changed to the inlined version.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class NestedClassGenerator {
   protected static final CtClass[] EMPTY_PARAMS = new CtClass[0];
   private static final String UNSUPPORTED = UnsupportedOperationException.class.getName();

   private final ClassPool classPool;
   private final Map<String, CtClass> newInterceptorsByOldName;
   private final Set<String> newInterceptorsNames;
   private final CtClass baseInterceptor;
   private final Map<String, String> replacedClassNames;
   private final Map<String, String> replacedSlashedNames;
   private final CtClass newInnerClass;
   private final CtClass oldInnerClass;
   private final ConstPool oldConstPool;
   private final ConstPool newConstPool;
   private final CtClass helperCtClass;

   public NestedClassGenerator(ClassPool classPool, Map<String, CtClass> newInterceptorsByOldName,
                               CtClass baseInterceptor, CtClass oldInnerClass, CtClass newInnerClass,
                               Map<String, String> replacedClassNames, Map<String, String> replacedSlashedNames) throws NotFoundException {
      this.classPool = classPool;
      this.newInterceptorsByOldName = newInterceptorsByOldName;
      this.newInterceptorsNames = newInterceptorsByOldName.values().stream().map(c -> c.getName()).collect(Collectors.toSet());
      this.baseInterceptor = baseInterceptor;
      this.oldInnerClass = oldInnerClass;
      this.newInnerClass = newInnerClass;
      this.oldConstPool = oldInnerClass.getClassFile().getConstPool();
      this.newConstPool = newInnerClass.getClassFile().getConstPool();
      this.replacedClassNames = replacedClassNames;
      this.replacedSlashedNames = replacedSlashedNames;
      this.helperCtClass = classPool.get(Helper.CLASSNAME);
   }

   public void run() throws NotFoundException, CannotCompileException, IOException, BadBytecode {
      ConstPool newConstPool = newInnerClass.getClassFile().getConstPool();

      CtClass superclass = oldInnerClass.getSuperclass();
      // with non-inner superclass, do not replace the name
      String replaced = superclass.getDeclaringClass() == null ? null : replacedClassNames.get(superclass.getName());
      newInnerClass.setSuperclass(replaced != null ? classPool.get(replaced) : superclass);
      for (CtClass iface : oldInnerClass.getInterfaces()) {
         replaced = replacedClassNames.get(iface.getName());
         newInnerClass.addInterface(replaced != null ? classPool.get(replaced) : iface);
      }

      String sourceFile = oldInnerClass.getClassFile().getSourceFile();
      if (sourceFile != null) {
         newInnerClass.getClassFile().addAttribute(new SourceFileAttribute(newConstPool, sourceFile));
      }

      AnnotationsAttribute annotations = (AnnotationsAttribute) oldInnerClass.getClassFile2().getAttribute(AnnotationsAttribute.visibleTag);
      if (annotations != null) {
         newInnerClass.getClassFile().getAttributes().add(annotations.copy(newConstPool, replacedSlashedNames));
      }

      Bytecode staticCtorCode = new Bytecode(newConstPool);

      for (CtField field : oldInnerClass.getDeclaredFields()) {
         CtClass fieldType = field.getType();
         String newFieldType = replacedClassNames.get(fieldType.getName());
         if (newFieldType != null) {
            fieldType = classPool.get(newFieldType);
         }
         CtField newField = new CtField(fieldType, field.getName(), newInnerClass);
         int modifiers = field.getModifiers();
         newField.setModifiers(field.getModifiers());
         if (Modifier.isStatic(modifiers)) {
            Helper.addCopyField(staticCtorCode, field, newField, null, -1, baseInterceptor);
         }

         newInnerClass.addField(newField);
      }

      if (staticCtorCode.get().length > 0) {
         CtConstructor staticCtor = new CtConstructor(EMPTY_PARAMS, newInnerClass);
         staticCtor.getMethodInfo().setName("<clinit>");
         staticCtor.setModifiers(Modifier.PUBLIC | Modifier.STATIC);
         staticCtorCode.addReturn(CtClass.voidType);
         staticCtorCode.setMaxLocals(true, EMPTY_PARAMS, 0);
         staticCtor.getMethodInfo().setCodeAttribute(staticCtorCode.toCodeAttribute());
         newInnerClass.addConstructor(staticCtor);
      }

      List<BootstrapMethodsAttribute.BootstrapMethod> newBootstrapMethods = new ArrayList<>();

      for (CtConstructor ctor : oldInnerClass.getDeclaredConstructors()) {
         CtConstructor newCtor = new CtConstructor(Helper.replaceInParams(ctor.getParameterTypes(), replacedClassNames, classPool), newInnerClass);
         newCtor.setModifiers(ctor.getModifiers());
         CodeAttribute codeAttribute = ctor.getMethodInfo().getCodeAttribute();
         newCtor.getMethodInfo().setCodeAttribute(copyCode(false, codeAttribute, newBootstrapMethods));
         newInnerClass.addConstructor(newCtor);
      }

      for (CtMethod m : oldInnerClass.getDeclaredMethods()) {
         CtClass returnType = m.getReturnType();
         String replacedReturnType = replacedClassNames.get(returnType.getName());
         if (replacedReturnType != null) {
            returnType = classPool.get(replacedReturnType);
         }
         CtMethod newMethod = new CtMethod(returnType, m.getName(), Helper.replaceInParams(m.getParameterTypes(), replacedClassNames, classPool), newInnerClass);
         newMethod.setModifiers(m.getModifiers());
         CodeAttribute codeAttribute = m.getMethodInfo().getCodeAttribute();
         if (codeAttribute != null) {
            newMethod.getMethodInfo().setCodeAttribute(copyCode(Modifier.isStatic(m.getModifiers()), codeAttribute, newBootstrapMethods));
         }
         newInnerClass.addMethod(newMethod);
      }

      if (newBootstrapMethods.size() > 0) {
         newInnerClass.getClassFile().addAttribute(new BootstrapMethodsAttribute(newConstPool,
               newBootstrapMethods.toArray(new BootstrapMethodsAttribute.BootstrapMethod[newBootstrapMethods.size()])));
      }

      generateCopyCtor();
   }

   private void generateCopyCtor() throws NotFoundException, CannotCompileException {
      CtClass[] parameters = {oldInnerClass, newInnerClass.getDeclaringClass(), helperCtClass};
      Bytecode bytecode = new Bytecode(newInnerClass.getClassFile().getConstPool());
      boolean hasCopyingSuperCtor = false;
      CtClass oldSuperclass = oldInnerClass.getSuperclass();
      CtClass newSuperclass = newInnerClass.getSuperclass();
      boolean canCopy = false;
      if (!oldSuperclass.getName().equals(newSuperclass.getName())) {
         // replaced class, we may use generated copy ctor
         bytecode.addAload(0);
         bytecode.addAload(1);
         bytecode.addAload(2);
         bytecode.addAload(3);
         CtClass superClassDeclaring = Helper.getOuterInterceptor(oldSuperclass, newInterceptorsByOldName);
         bytecode.addInvokespecial(newSuperclass, "<init>", Descriptor.ofConstructor(new CtClass[] {oldSuperclass, superClassDeclaring, helperCtClass}));
         canCopy = true;
         hasCopyingSuperCtor = true;
      } else {
         // we have to find appropriate superctor, but there may be no superctor without sideeffect
         // or it may require strange arguments. Therefore we'll just use first fitting
         for (CtConstructor ctor : oldSuperclass.getDeclaredConstructors()) {
            CtClass[] parameterTypes = ctor.getParameterTypes();
            if (parameterTypes.length == 0) {
               bytecode.addAload(0);
               bytecode.addInvokespecial(newSuperclass, "<init>", "()V");
               canCopy = true;
               break;
            } else if (parameterTypes.length == 1 && oldInnerClass.subtypeOf(parameterTypes[0])) {
               bytecode.addAload(0);
               bytecode.addAload(1);
               bytecode.addInvokespecial(newSuperclass, "<init>", Descriptor.ofConstructor(parameterTypes));
               canCopy = true;
               break;
            }
         }
      }
      if (canCopy) {
         for (CtClass ctClass = oldInnerClass; ctClass != null; ctClass = ctClass.getSuperclass()) {
            CtClass declaringInterceptor = Helper.getOuterInterceptor(ctClass, newInterceptorsByOldName);
            for (CtField field : ctClass.getDeclaredFields()) {
               int modifiers = field.getModifiers();
               if (Modifier.isStatic(modifiers)) {
                  continue;
               } else if (Modifier.isFinal(modifiers) && ctClass != oldInnerClass) {
                  // TODO: we have no control over superclass, but we can't copy into final superfield
                  continue;
               }
               String className = replacedClassNames.get(ctClass.getName());
               if (className == null) {
                  Helper.addCopyField(bytecode, field, field, declaringInterceptor, 2, baseInterceptor);
               } else {
                  CtField newField = classPool.get(className).getDeclaredField(field.getName());
                  Helper.addCopyField(bytecode, field, newField, declaringInterceptor, 2, baseInterceptor);
               }
            }
            // we've copied the class in superconstructor
            if (hasCopyingSuperCtor) {
               break;
            }
         }
         bytecode.addReturn(CtClass.voidType);
      } else {
         bytecode.addNew(UNSUPPORTED);
         bytecode.addOpcode(Opcode.DUP);
         bytecode.addInvokespecial(UNSUPPORTED, "<init>", "()V");
         bytecode.addOpcode(Opcode.ATHROW);
      }
      CtConstructor ctor = new CtConstructor(parameters, newInnerClass);
      ctor.setModifiers(Modifier.PUBLIC);
      bytecode.setMaxLocals(false, parameters, 0);
      ctor.getMethodInfo().setCodeAttribute(bytecode.toCodeAttribute());
      newInnerClass.addConstructor(ctor);
   }

   public CodeAttribute copyCode(boolean isStatic, CodeAttribute codeAttribute, List<BootstrapMethodsAttribute.BootstrapMethod> newBootstrapMethods) throws BadBytecode, NotFoundException, CannotCompileException {
      CodeAttribute copy = (CodeAttribute) codeAttribute.copy(newConstPool, replacedSlashedNames);
      Simulator simulator = new Simulator();
      simulator.run(isStatic, -1, oldConstPool, codeAttribute);
      CodeIterator oldIterator = codeAttribute.iterator();
      CodeIterator newIterator = copy.iterator();
      while (newIterator.hasNext()) {
         int readPc = oldIterator.next();
         int writePc = newIterator.next();
         int op = newIterator.byteAt(writePc);
         int index;
         String className, fieldName, methodName, type;
         switch (op) {
            case Opcode.INVOKESPECIAL:
               index = oldIterator.u16bitAt(readPc + 1);
               className = oldConstPool.getMethodrefClassName(index);
               CtClass clazz = classPool.get(className);
               // in invoke special, don't replace calls to non-inner classes as these apply to our supertype
               if (clazz.getDeclaringClass() == null) {
                  fieldName = oldConstPool.getMethodrefName(index);
                  type = oldConstPool.getMethodrefType(index);
                  int ref = newConstPool.addMethodrefInfo(newConstPool.addClassInfo(className), fieldName, type);
                  newIterator.write16bit(ref, writePc + 1);
               }
               break;
            case Opcode.INVOKEVIRTUAL:
            case Opcode.INVOKESTATIC:
               index = newIterator.u16bitAt(writePc + 1);
               // there can be static methods in interfaces, but we don't care about these
               if (newConstPool.getTag(index) == Constants.METHOD_REF_INFO) {
                  className = newConstPool.getMethodrefClassName(index);
                  if (newInterceptorsNames.contains(className)) {
                     int classInfo = newConstPool.getMethodrefClass(index);
                     methodName = newConstPool.getMethodrefName(index);
                     type = newConstPool.getMethodrefType(index);
                     int methodRef = newConstPool.addMethodrefInfo(classInfo, methodName, Descriptor.rename(type, replacedSlashedNames));
                     newIterator.write16bit(methodRef, writePc + 1);
                  }
               }
               break;
            case Opcode.INVOKEDYNAMIC:
               index = oldIterator.u16bitAt(readPc + 1);
               int bootstrap = oldConstPool.getInvokeDynamicBootstrap(index);
               int bootstrapNameAndType = oldConstPool.getInvokeDynamicNameAndType(index);
               int bootstrapName = oldConstPool.getNameAndTypeName(bootstrapNameAndType);
               int bootstrapType = oldConstPool.getNameAndTypeDescriptor(bootstrapNameAndType);
               int newBootstrapNameAndType = newConstPool.addNameAndTypeInfo(oldConstPool.getUtf8Info(bootstrapName),
                     Helper.replaceInDescriptor(classPool, replacedClassNames, oldConstPool.getUtf8Info(bootstrapType)));
               BootstrapMethodsAttribute.BootstrapMethod bootstrapMethod = getBootstrapMethod(oldInnerClass, bootstrap);
               Helper.MethodCopier methodCopier = m -> {
                  String replaced = replacedClassNames.get(m.getDeclaringClass().getName());
                  if (replaced == null) {
                     replaced = m.getDeclaringClass().getName();
                  }
                  String descriptor = Helper.replaceInDescriptor(classPool, replacedClassNames, m.getSignature());
                  return new NameAndDescriptor(replaced, m.getName(), descriptor);
               };
               int newBootstrapMethodHandle = Helper.copyMethodHandle(classPool, oldConstPool, newConstPool, bootstrapMethod.methodRef, replacedClassNames, methodCopier);
               int[] newBootstrapMethodArguments = Helper.copyBootstrapMethodArguments(classPool, oldConstPool, newConstPool, bootstrapMethod, replacedClassNames, methodCopier);
               int newBootstrapIndex = newBootstrapMethods.size();
               newBootstrapMethods.add(new BootstrapMethodsAttribute.BootstrapMethod(newBootstrapMethodHandle, newBootstrapMethodArguments));
               int newInvokeDynamic = newConstPool.addInvokeDynamicInfo(newBootstrapIndex, newBootstrapNameAndType);
               newIterator.write16bit(newInvokeDynamic, writePc + 1);
               break;
            case Opcode.GETFIELD:
               index = newIterator.u16bitAt(writePc + 1);
               className = newConstPool.getFieldrefClassName(index);
               if (newInterceptorsNames.contains(className)) {
                  // the method was made private, we need accessor for external use
                  fieldName = newConstPool.getFieldrefName(index);
                  type = newConstPool.getFieldrefType(index);
                  String accessorName = "access$" + fieldName;
                  CtClass typeClass = Descriptor.toCtClass(type, classPool);
                  CtClass newInterceptor = classPool.get(className);
                  CtClass[] accessorParameters = { newInterceptor };
                  int methodRef = newConstPool.addMethodrefInfo(newConstPool.addClassInfo(newInterceptor), accessorName, Descriptor.ofMethod(typeClass, accessorParameters));
                  newIterator.writeByte(Opcode.INVOKESTATIC, writePc);
                  newIterator.write16bit(methodRef, writePc + 1);
                  generateFieldAccessor(newInterceptor, accessorName, typeClass, accessorParameters, fieldName, type);
               }
               break;
         }
      }
      return copy;
   }

   private void generateFieldAccessor(CtClass newInterceptor, String accessorName, CtClass fieldClass, CtClass[] parameters, String fieldName, String fieldType) throws NotFoundException, CannotCompileException {
      if (checkGenerated(newInterceptor, accessorName, parameters)) {
         return;
      }
      CtMethod accessor = new CtMethod(fieldClass, accessorName, parameters, newInterceptor);
      // flattened parent can be in different package than the inner class
      accessor.setModifiers(Modifier.STATIC | Modifier.PUBLIC | Modifier.FINAL);
      Bytecode bytecode = new Bytecode(newInterceptor.getClassFile().getConstPool());
      bytecode.addLoad(0, newInterceptor);
      bytecode.addGetfield(newInterceptor, fieldName, fieldType);
      bytecode.addReturn(fieldClass);
      bytecode.setMaxLocals(true, parameters, 0);

      accessor.getMethodInfo().setCodeAttribute(bytecode.toCodeAttribute());
      newInterceptor.addMethod(accessor);
   }

   private boolean checkGenerated(CtClass newInterceptor, String accessorName, CtClass[] parameters) throws NotFoundException {
      for (CtMethod m : newInterceptor.getDeclaredMethods()) {
         if (m.getName().equals(accessorName) && Arrays.equals(m.getParameterTypes(), parameters)) {
            // accessor already generated
            return true;
         }
      }
      return false;
   }

   public BootstrapMethodsAttribute.BootstrapMethod getBootstrapMethod(CtClass oldInnerClass, int bootstrap) {
      BootstrapMethodsAttribute bma = (BootstrapMethodsAttribute) oldInnerClass.getClassFile().getAttribute(BootstrapMethodsAttribute.tag);
      if (bma == null) {
         throw new IllegalArgumentException("No bootstrap methods");
      }
      return bma.getMethods()[bootstrap];
   }

}
