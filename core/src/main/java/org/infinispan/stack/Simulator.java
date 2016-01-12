package org.infinispan.stack;

import javassist.bytecode.BadBytecode;
import javassist.bytecode.CodeAttribute;
import javassist.bytecode.CodeIterator;
import javassist.bytecode.ConstPool;
import javassist.bytecode.Descriptor;
import javassist.bytecode.ExceptionTable;
import javassist.bytecode.Mnemonic;
import javassist.bytecode.Opcode;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;

/**
 * Simulates all possible control flows when invoking a method, with one parameter
 * being marked as 'correct' and all other values considered garbage.
 * After the simulation is complete, {@link #checkStack(int, int)}
 * can be used to find out whether it is possible that at instruction PC the value N deep in the stack
 * is correct or whether it can be a garbage.
 * It's possible that this method has some false positives as certain branching may never occur but the
 * simulator considers it as possibility, or if the parameter is passed to another method and returned from
 * it (all return values of other methods are considered garbage).
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
class Simulator {
   private static final Log log = LogFactory.getLog(Simulator.class);

   private static final int THIS = -1;
   private static final int PRISTINE = -2;
   private static final int OTHER = -3;
   private static final int[] NONE = new int[] {};
   private static final int[] WORD = new int[] { OTHER };
   private static final int[] DWORD = new int[] { OTHER, OTHER };

   private Queue<State> unprocessed = new ArrayDeque<>();
   private Map<Integer, Set<State>> states = new TreeMap<>();

   public void run(boolean isStatic, int commandVar, ConstPool constPool, CodeAttribute codeAttribute) throws BadBytecode {
      State initial = new State(isStatic, codeAttribute.getMaxLocals(), commandVar);
      unprocessed.add(initial);
      add(initial);
      CodeIterator iterator = codeAttribute.iterator();
      do {
         {
            State s;
            while ((s = unprocessed.poll()) != null) {
               try {
                  processState(s, codeAttribute, iterator, constPool);
               } catch (RuntimeException e) {
                  log.error("Failure processing state " + s);
                  printStates();
                  throw e;
               }
            }
         }
         ExceptionTable exceptionTable = codeAttribute.getExceptionTable();
         for (int i = 0; i < exceptionTable.size(); ++i) {
            int handler = exceptionTable.handlerPc(i);
            for (int pc = exceptionTable.startPc(i); pc < exceptionTable.endPc(i); ++pc) {
               Set<State> statesOn = states.get(pc);
               // exception handler may cover states reachable only by exception
               if (statesOn != null) {
                  // copy states because we add during iteration
                  for (State s : new HashSet<>(statesOn)) {
                     add(s.next(handler, WORD)); // stack contains only the reference to the exception
                  }
               }
            }
         }
      } while (!unprocessed.isEmpty());
   }

   public boolean checkStack(int pc, int stackDepth) {
      Set<State> statesOn = this.states.get(pc);
      if (statesOn == null) {
         throw new IllegalStateException("The call on PC " + pc + " is unreachable");
      }
      for (State s : statesOn) {
         if (s.stack[s.stack.length - stackDepth - 1] != PRISTINE) {
            return false;
         }
      }
      return true;
   }

   public boolean checkLocals(int pc, int localVar) {
      Set<State> statesOn = this.states.get(pc);
      if (statesOn == null) {
         throw new IllegalStateException("The call on PC " + pc + " is unreachable");
      }
      for (State s : statesOn) {
         if (s.locals[localVar] != PRISTINE) {
            return false;
         }
      }
      return true;
   }

   private void processState(State s, CodeAttribute codeAttribute, CodeIterator iterator, ConstPool constPool) throws BadBytecode {
      int stackSize = s.stack.length;
      int tableStart, numLabels;
      int[] pushedValues;
      String desc;
      // these two are needed for lookAhead()
      iterator.move(s.pc);
      iterator.next();
      int op = iterator.byteAt(s.pc);
      switch (op) {
         case Opcode.JSR_W:
            // eventually we'll return back
            add(s.next(s.pc + iterator.s32bitAt(s.pc + 1), 0, iterator.lookAhead()));
            break;
         case Opcode.JSR:
            add(s.next(s.pc + iterator.s16bitAt(s.pc + 1), 0, iterator.lookAhead()));
            break;
         case Opcode.GOTO_W:
            add(s.next(s.pc + iterator.s32bitAt(s.pc + 1)));
            break;
         case Opcode.GOTO:
            add(s.next(s.pc + iterator.s16bitAt(s.pc + 1)));
            break;
         case Opcode.IFEQ:
         case Opcode.IFGE:
         case Opcode.IFGT:
         case Opcode.IFLE:
         case Opcode.IFLT:
         case Opcode.IFNE:
         case Opcode.IFNONNULL:
         case Opcode.IFNULL:
            add(s.next(iterator.lookAhead(), 1));
            add(s.next(s.pc + iterator.s16bitAt(s.pc + 1), 1));
            break;
         case Opcode.IF_ACMPEQ:
         case Opcode.IF_ACMPNE:
         case Opcode.IF_ICMPEQ:
         case Opcode.IF_ICMPGE:
         case Opcode.IF_ICMPGT:
         case Opcode.IF_ICMPLE:
         case Opcode.IF_ICMPLT:
         case Opcode.IF_ICMPNE:
            add(s.next(iterator.lookAhead(), 2));
            add(s.next(s.pc + iterator.s16bitAt(s.pc + 1), 2));
            break;
         case Opcode.LOOKUPSWITCH:
            tableStart = (s.pc & ~3) + 4;
            add(s.next(s.pc + iterator.s32bitAt(tableStart)));
            numLabels = iterator.s32bitAt(tableStart + 4);
            for (int i = 0; i < numLabels; ++i) {
               add(s.next(s.pc + iterator.s32bitAt(tableStart + 12 + 8*i)));
            }
            break;
         case Opcode.TABLESWITCH:
            tableStart = (s.pc & ~3) + 4;
            add(s.next(s.pc + iterator.s32bitAt(tableStart)));
            int low = iterator.s32bitAt(tableStart + 4);
            int high = iterator.s32bitAt(tableStart + 8);
            numLabels = high - low + 1;
            for (int i = 0; i < numLabels; ++i) {
               add(s.next(s.pc + iterator.s32bitAt(tableStart + 12 + 4*i)));
            }
            break;
         case Opcode.RET:
            s.next(s.locals[s.wide ? iterator.u16bitAt(s.pc + 1) : iterator.byteAt(s.pc + 1)]);
            break;
         case Opcode.ATHROW:
         case Opcode.RETURN:
         case Opcode.ARETURN:
         case Opcode.IRETURN:
         case Opcode.LRETURN:
         case Opcode.DRETURN:
         case Opcode.FRETURN:
            break;
         case Opcode.AASTORE:
         case Opcode.BASTORE:
         case Opcode.CASTORE:
         case Opcode.FASTORE:
         case Opcode.IASTORE:
         case Opcode.SASTORE:
            add(s.next(iterator.lookAhead(), 3));
            break;
         case Opcode.DASTORE:
         case Opcode.LASTORE:
            add(s.next(iterator.lookAhead(), 4));
            break;
         case Opcode.ALOAD:
         case Opcode.FLOAD:
         case Opcode.ILOAD:
            add(s.next(iterator.lookAhead(), 0, s.wide ? s.locals[iterator.u16bitAt(s.pc + 1)] : s.locals[iterator.byteAt(s.pc + 1)]));
            break;
         case Opcode.ALOAD_0:
         case Opcode.FLOAD_0:
         case Opcode.ILOAD_0:
            add(s.next(iterator.lookAhead(), 0, s.locals[0]));
            break;
         case Opcode.ALOAD_1:
         case Opcode.FLOAD_1:
         case Opcode.ILOAD_1:
            add(s.next(iterator.lookAhead(), 0, s.locals[1]));
            break;
         case Opcode.ALOAD_2:
         case Opcode.FLOAD_2:
         case Opcode.ILOAD_2:
            add(s.next(iterator.lookAhead(), 0, s.locals[2]));
            break;
         case Opcode.ALOAD_3:
         case Opcode.FLOAD_3:
         case Opcode.ILOAD_3:
            add(s.next(iterator.lookAhead(), 0, s.locals[3]));
            break;
         case Opcode.DLOAD:
         case Opcode.DLOAD_0:
         case Opcode.DLOAD_1:
         case Opcode.DLOAD_2:
         case Opcode.DLOAD_3:
         case Opcode.LLOAD:
         case Opcode.LLOAD_0:
         case Opcode.LLOAD_1:
         case Opcode.LLOAD_2:
         case Opcode.LLOAD_3:
         case Opcode.DCONST_0:
         case Opcode.DCONST_1:
         case Opcode.LCONST_0:
         case Opcode.LCONST_1:
         case Opcode.LDC2_W:
            add(s.next(iterator.lookAhead(), 0, DWORD));
            break;
         case Opcode.ACONST_NULL:
         case Opcode.BIPUSH:
         case Opcode.FCONST_0:
         case Opcode.FCONST_1:
         case Opcode.FCONST_2:
         case Opcode.ICONST_0:
         case Opcode.ICONST_1:
         case Opcode.ICONST_2:
         case Opcode.ICONST_3:
         case Opcode.ICONST_4:
         case Opcode.ICONST_5:
         case Opcode.ICONST_M1:
         case Opcode.LDC:
         case Opcode.LDC_W:
         case Opcode.NEW:
         case Opcode.SIPUSH:
            add(s.next(iterator.lookAhead(), 0, OTHER));
            break;
         case Opcode.ANEWARRAY:
         case Opcode.ARRAYLENGTH:
         case Opcode.INSTANCEOF:
         case Opcode.NEWARRAY:
            add(s.next(iterator.lookAhead(), 1, OTHER));
            break;
         case Opcode.D2F:
         case Opcode.D2I:
         case Opcode.L2F:
         case Opcode.L2I:
         case Opcode.AALOAD:
         case Opcode.BALOAD:
         case Opcode.CALOAD:
         case Opcode.FALOAD:
         case Opcode.FADD:
         case Opcode.FCMPG:
         case Opcode.FCMPL:
         case Opcode.FDIV:
         case Opcode.FMUL:
         case Opcode.FREM:
         case Opcode.FSUB:
         case Opcode.IADD:
         case Opcode.IALOAD:
         case Opcode.IAND:
         case Opcode.IDIV:
         case Opcode.IMUL:
         case Opcode.IOR:
         case Opcode.IREM:
         case Opcode.ISHL:
         case Opcode.ISHR:
         case Opcode.ISUB:
         case Opcode.SALOAD:
            add(s.next(iterator.lookAhead(), 2, OTHER));
            break;
         case Opcode.DALOAD:
         case Opcode.LALOAD:
            add(s.next(iterator.lookAhead(), 2, DWORD));
            break;
         case Opcode.DADD:
         case Opcode.DDIV:
         case Opcode.DMUL:
         case Opcode.DREM:
         case Opcode.DSUB:
         case Opcode.LADD:
         case Opcode.LAND:
         case Opcode.LDIV:
         case Opcode.LMUL:
         case Opcode.LOR:
         case Opcode.LREM:
         case Opcode.LSUB:
         case Opcode.LXOR:
            add(s.next(iterator.lookAhead(), 4, DWORD));
            break;
         case Opcode.LSHL:
         case Opcode.LSHR:
         case Opcode.LUSHR:
            add(s.next(iterator.lookAhead(), 3, DWORD));
            break;
         case Opcode.DCMPG:
         case Opcode.DCMPL:
         case Opcode.LCMP:
            add(s.next(iterator.lookAhead(), 4, OTHER));
            break;
         case Opcode.ASTORE:
         case Opcode.FSTORE:
         case Opcode.ISTORE:
            add(s.next(iterator.lookAhead(), 1, s.wide ? iterator.u16bitAt(s.pc + 1) : iterator.byteAt(s.pc + 1), s.stackTop(), false));
            break;
         case Opcode.ASTORE_0:
         case Opcode.FSTORE_0:
         case Opcode.ISTORE_0:
            add(s.next(iterator.lookAhead(), 1, 0, s.stackTop(), false));
            break;
         case Opcode.ASTORE_1:
         case Opcode.FSTORE_1:
         case Opcode.ISTORE_1:
            add(s.next(iterator.lookAhead(), 1, 1, s.stackTop(), false));
            break;
         case Opcode.ASTORE_2:
         case Opcode.FSTORE_2:
         case Opcode.ISTORE_2:
            add(s.next(iterator.lookAhead(), 1, 2, s.stackTop(), false));
            break;
         case Opcode.ASTORE_3:
         case Opcode.FSTORE_3:
         case Opcode.ISTORE_3:
            add(s.next(iterator.lookAhead(), 1, 3, s.stackTop(), false));
            break;
         case Opcode.DSTORE:
         case Opcode.LSTORE:
            add(s.next(iterator.lookAhead(), 2, s.wide ? iterator.u16bitAt(s.pc + 1) : iterator.byteAt(s.pc + 1), OTHER, true));
            break;
         case Opcode.DSTORE_0:
         case Opcode.LSTORE_0:
            add(s.next(iterator.lookAhead(), 2, 0, OTHER, true));
            break;
         case Opcode.DSTORE_1:
         case Opcode.LSTORE_1:
            add(s.next(iterator.lookAhead(), 2, 1, OTHER, true));
            break;
         case Opcode.DSTORE_2:
         case Opcode.LSTORE_2:
            add(s.next(iterator.lookAhead(), 2, 2, OTHER, true));
            break;
         case Opcode.DSTORE_3:
         case Opcode.LSTORE_3:
            add(s.next(iterator.lookAhead(), 2, 3, OTHER, true));
            break;
         case Opcode.D2L:
         case Opcode.DNEG:
         case Opcode.L2D:
         case Opcode.CHECKCAST:
         case Opcode.F2I:
         case Opcode.FNEG:
         case Opcode.I2B:
         case Opcode.I2C:
         case Opcode.I2F:
         case Opcode.I2S:
         case Opcode.INEG:
         case Opcode.IUSHR:
         case Opcode.IXOR:
         case Opcode.LNEG:
         case Opcode.NOP:
            add(s.next(iterator.lookAhead()));
            break;
         case Opcode.F2D:
         case Opcode.F2L:
         case Opcode.I2L:
         case Opcode.I2D:
            add(s.next(iterator.lookAhead(), 1, DWORD));
            break;
         case Opcode.DUP:
            add(s.next(iterator.lookAhead(), 0, s.stackTop()));
            break;
         case Opcode.DUP2:
            add(s.next(iterator.lookAhead(), 0, new int[] { s.stack[stackSize - 2], s.stack[stackSize - 1]}));
            break;
         case Opcode.DUP2_X1:
            add(s.next(iterator.lookAhead(), 3, new int[] { s.stack[stackSize - 2], s.stack[stackSize - 1], s.stack[stackSize - 3], s.stack[stackSize - 2], s.stack[stackSize - 1]}));
            break;
         case Opcode.DUP2_X2:
            add(s.next(iterator.lookAhead(), 4, new int[] { s.stack[stackSize - 2], s.stack[stackSize - 1], s.stack[stackSize - 4], s.stack[stackSize - 3], s.stack[stackSize - 2], s.stack[stackSize - 1]}));
            break;
         case Opcode.DUP_X1:
            add(s.next(iterator.lookAhead(), 2, new int[] { s.stack[stackSize - 1], s.stack[stackSize - 2], s.stack[stackSize - 1] }));
            break;
         case Opcode.DUP_X2:
            add(s.next(iterator.lookAhead(), 3, new int[] { s.stack[stackSize - 1], s.stack[stackSize - 3], s.stack[stackSize - 2], s.stack[stackSize - 1] }));
            break;
         case Opcode.GETFIELD:
            add(s.next(iterator.lookAhead(), 1, getReturnValues(getNumWords(constPool.getFieldrefType(iterator.u16bitAt(s.pc + 1))))));
            break;
         case Opcode.GETSTATIC:
            add(s.next(iterator.lookAhead(), 0, getReturnValues(getNumWords(constPool.getFieldrefType(iterator.u16bitAt(s.pc + 1))))));
            break;
         case Opcode.IINC:
            add(s.next(iterator.lookAhead(), 0, s.wide ? iterator.u16bitAt(s.pc + 1) : iterator.byteAt(s.pc + 1), OTHER, false));
            break;
         case Opcode.INVOKEVIRTUAL :
         case Opcode.INVOKESPECIAL :
            desc = constPool.getMethodrefType(iterator.u16bitAt(s.pc + 1));
            pushedValues = getReturnValues(getNumWords(String.valueOf(desc.charAt(desc.length() - 1))));
            add(s.next(iterator.lookAhead(), Descriptor.paramSize(desc) + 1, pushedValues));
            break;
         case Opcode.INVOKESTATIC :
            switch (constPool.getTag(iterator.u16bitAt(s.pc + 1))) {
               case Constants.METHOD_REF_INFO:
                  desc = constPool.getMethodrefType(iterator.u16bitAt(s.pc + 1));
                  break;
               case Constants.IFACE_METHOD_REF_INFO:
                  desc = constPool.getInterfaceMethodrefType(iterator.u16bitAt(s.pc + 1));
                  break;
               default:
                  throw new IllegalArgumentException();
            }
            pushedValues = getReturnValues(getNumWords(String.valueOf(desc.charAt(desc.length() - 1))));
            add(s.next(iterator.lookAhead(), Descriptor.paramSize(desc), pushedValues));
            break;
         case Opcode.INVOKEINTERFACE :
            desc = constPool.getInterfaceMethodrefType(iterator.u16bitAt(s.pc + 1));
            pushedValues = getReturnValues(getNumWords(String.valueOf(desc.charAt(desc.length() - 1))));
            add(s.next(iterator.lookAhead(), Descriptor.paramSize(desc) + 1, pushedValues));
            break;
         case Opcode.INVOKEDYNAMIC :
            // not sure about this
            desc = constPool.getInvokeDynamicType(iterator.u16bitAt(s.pc + 1));
            pushedValues = getReturnValues(getNumWords(String.valueOf(desc.charAt(desc.length() - 1))));
            add(s.next(iterator.lookAhead(), Descriptor.paramSize(desc), pushedValues));
            break;
         case Opcode.MONITORENTER:
         case Opcode.MONITOREXIT:
         case Opcode.POP:
            add(s.next(iterator.lookAhead(), 1));
            break;
         case Opcode.POP2:
            add(s.next(iterator.lookAhead(), 2));
            break;
         case Opcode.MULTIANEWARRAY:
            add(s.next(iterator.lookAhead(), iterator.byteAt(s.pc + 3), OTHER));
            break;
         case Opcode.PUTFIELD:
            add(s.next(iterator.lookAhead(), getNumWords(constPool.getFieldrefType(iterator.u16bitAt(s.pc + 1))) + 1));
            break;
         case Opcode.PUTSTATIC:
            add(s.next(iterator.lookAhead(), getNumWords(constPool.getFieldrefType(iterator.u16bitAt(s.pc + 1)))));
            break;
         case Opcode.SWAP:
            add(s.next(iterator.lookAhead(), 2, new int[] { s.stack[stackSize - 1], s.stack[stackSize - 2]}));
            break;
         case Opcode.WIDE:
            add(s.next(iterator.lookAhead(), true));
            break;
         default:
            throw new IllegalArgumentException("Unknown instruction " + Mnemonic.OPCODE[op]);
      }
   }

   private int[] getReturnValues(int numWords) {
      switch (numWords) {
         case 0:
            return NONE;
         case 1:
            return WORD;
         case 2:
            return DWORD;
         default:
            throw new IllegalArgumentException();
      }
   }

   private int getNumWords(String type) {
      return "D".equals(type) || "J".equals(type) ? 2 : ("V".equals(type) ? 0 : 1);
   }

   private void add(State state) {
      Set<State> statesOn = states.get(state.pc);
      if (statesOn == null) {
         states.put(state.pc, statesOn = new HashSet<>());
      }
      if (statesOn.add(state)) {
         unprocessed.add(state);
      }
   }

   public void printStates() {
      for (Map.Entry<Integer, Set<State>> entry : states.entrySet()) {
         for (State s : entry.getValue()) {
            log.info(s.toString());
         }
      }
   }

   private static class State {
      private static final int[] EMPTY_STACK = new int[0];
      private final int pc;
      private final int[] stack;
      private final int[] locals;
      private final boolean wide;
      // just for trackback purposes
      private final State prev;

      private State(State prev, int pc, int[] stack, int[] locals, boolean wide) {
         assert pc >= 0;
         assert stack != null;
         assert locals != null;
         this.prev = prev;
         this.pc = pc;
         this.stack = stack;
         this.locals = locals;
         this.wide = wide;
      }

      public State(boolean isStatic, int maxLocals, int commandVar) {
         pc = 0;
         stack = EMPTY_STACK;
         locals = new int[maxLocals];
         Arrays.fill(locals, OTHER);
         if (isStatic) {
            if (commandVar >= 0) {
               locals[commandVar] = PRISTINE;
            }
         } else {
            locals[0] = THIS;
            if (commandVar >= 0) {
               locals[commandVar + 1] = PRISTINE;
            }
         }
         wide = false;
         prev = null;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         State state = (State) o;

         if (pc != state.pc) return false;
         if (!Arrays.equals(stack, state.stack)) return false;
         if (!Arrays.equals(locals, state.locals)) return false;
         return wide == state.wide;
      }

      @Override
      public int hashCode() {
         int result = pc;
         result = 31 * result + Arrays.hashCode(stack);
         result = 31 * result + Arrays.hashCode(locals);
         return result;
      }

      public State next(int newPc, int numPopped, int pushedValue) {
         int[] newStack = Arrays.copyOf(stack, stack.length - numPopped + 1);
         newStack[stack.length - numPopped] = pushedValue;
         return new State(this, newPc, newStack, locals, false);
      }

      public State next(int newPc, int numPopped, int[] pushedValues) {
         int[] newStack = Arrays.copyOf(stack, stack.length - numPopped + pushedValues.length);
         System.arraycopy(pushedValues, 0, newStack, stack.length - numPopped, pushedValues.length);
         return new State(this, newPc, newStack, locals, false);
      }

      public State next(int newPc) {
         return new State(this, newPc, stack, locals, false);
      }

      public State next(int newPc, int numPopped) {
         int[] newStack = Arrays.copyOf(stack, stack.length - numPopped);
         return new State(this, newPc, newStack, locals, false);
      }

      public State next(int newPc, boolean wide) {
         return new State(this, newPc, stack, locals, wide);
      }

      public int stackTop() {
         return stack[stack.length - 1];
      }

      public State next(int newPc, int numPopped, int local, int value, boolean dword) {
         int[] newLocals = Arrays.copyOf(locals, locals.length);
         newLocals[local] = value;
         if (dword) {
            newLocals[local + 1] = value;
         }
         int[] newStack = numPopped == 0 ? stack : Arrays.copyOf(stack, stack.length - numPopped);
         return new State(this, newPc, newStack, newLocals, false);
      }

      public State next(int newPc, int[] newStack) {
         return new State(this, newPc, newStack, locals, false);
      }

      @Override
      public String toString() {
         StringBuilder sb = new StringBuilder("pc=").append(pc).append(", pristine locals=");
         boolean firstPristine = true;
         for (int i = 0; i < locals.length; ++i) {
            if (locals[i] == PRISTINE) {
               if (!firstPristine) {
                  sb.append(", ");
               }
               sb.append(i);
               firstPristine = false;
            }
         }
         sb.append('/').append(locals.length).append(", stack=");
         for (int i = 0; i < stack.length; ++i) {
            switch (stack[i]) {
               case THIS:
                  sb.append('T');
                  break;
               case PRISTINE:
                  sb.append('X');
                  break;
               case OTHER:
                  sb.append('o');
                  break;
               default: //JSR address
                  sb.append('a');
            }
         }

         return sb.toString();
      }
   }
}
