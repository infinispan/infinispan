package org.infinispan.stack;

import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtField;
import javassist.CtMethod;
import javassist.CtNewConstructor;
import javassist.Modifier;
import javassist.NotFoundException;
import javassist.bytecode.Bytecode;
import javassist.bytecode.ClassFile;
import javassist.bytecode.Descriptor;
import javassist.bytecode.SourceFileAttribute;

import java.io.IOException;

/**
 * Generates a class that implements the interceptor interface but just delegates the call
 * to the actual interceptor instance.
 *
 * @author Radim Vansa &ltrvansa@redhat.com&gt;
 */
public class DelegatorGenerator {
   private static final String DELEGATE = "delegate";
   private final ClassPool classPool = ClassPool.getDefault();
   private final StackOptimizer so;
   private final CtClass newInterceptor;
   private CtClass newDelegator;
   private Class newJavaDelegator;

   public DelegatorGenerator(StackOptimizer so, CtClass newInterceptor) {
      this.so = so;
      this.newInterceptor = newInterceptor;
   }

   public void generate() throws CannotCompileException, NotFoundException, IOException {
      newDelegator = classPool.makeClass(newInterceptor.getName() + "$Delegator");
      Helper.removeAttribute(newDelegator.getClassFile().getAttributes(), SourceFileAttribute.tag);
      newDelegator.getClassFile().setMajorVersion(ClassFile.JAVA_8);

      newDelegator.setModifiers(Modifier.PUBLIC | Modifier.FINAL);
      newDelegator.addInterface(so.visitorClass);
      CtField delegateField = new CtField(newInterceptor, DELEGATE, newDelegator);
      delegateField.setModifiers(Modifier.PRIVATE | Modifier.FINAL);
      newDelegator.addField(delegateField);
      newDelegator.addConstructor(CtNewConstructor.make(new CtClass[] {newInterceptor}, Helper.EMPTY_PARAMS, "this.delegate = $1;", newDelegator));
      for (CtMethod delegatedMethod : so.visitorClass.getDeclaredMethods()) {
         CtClass returnType = delegatedMethod.getReturnType();
         CtClass[] parameterTypes = delegatedMethod.getParameterTypes();
         CtMethod method = new CtMethod(returnType, delegatedMethod.getName(), parameterTypes, newDelegator);
         method.setModifiers(Modifier.PUBLIC);
         method.setExceptionTypes(delegatedMethod.getExceptionTypes());
         Bytecode bytecode = new Bytecode(newDelegator.getClassFile().getConstPool());
         bytecode.addAload(0);
         bytecode.addGetfield(newDelegator, DELEGATE, Descriptor.of(newInterceptor));
         for (int i = 0; i < parameterTypes.length; ++i) {
            bytecode.addAload(i + 1);
         }
         CtClass[] newParameterTypes = Helper.prependClass(newInterceptor, parameterTypes);
         bytecode.addInvokestatic(newInterceptor, delegatedMethod.getName(), returnType, newParameterTypes);
         bytecode.addReturn(returnType);
         bytecode.setMaxLocals(false, parameterTypes, 0);
         method.getMethodInfo().addAttribute(bytecode.toCodeAttribute());
         newDelegator.addMethod(method);
      }

      if (Helper.DUMP_CLASSES != null) {
         newDelegator.writeFile(Helper.DUMP_CLASSES);
      }
      newJavaDelegator = newDelegator.toClass();
   }

   public CtClass getNewDelegator() {
      return newDelegator;
   }

   public Class getNewJavaDelegator() {
      return newJavaDelegator;
   }

}
