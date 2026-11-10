package org.infinispan.query.objectfilter.impl.syntax.update;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.invoke.MethodHandles;
import java.time.LocalDateTime;
import java.util.Map;

import org.infinispan.query.objectfilter.ParsingException;
import org.infinispan.query.objectfilter.impl.logging.Log;
import org.jboss.logging.Logger;
import org.testng.annotations.Test;

@Test(testName = "query.objectfilter.impl.syntax.update.UpdateValueTypeCheckerTest", groups = "functional")
public class UpdateValueTypeCheckerTest {

   private static final Log LOG = Logger.getMessageLogger(MethodHandles.lookup(), Log.class,
         UpdateValueTypeCheckerTest.class.getName());
   private static final QueryFunctionRegistry REGISTRY = QueryFunctionRegistry.standard();
   private static final Map<String, Class<?>> TYPES = Map.of(
         "rating", int.class,
         "name", String.class,
         "releaseDate", LocalDateTime.class,
         "published", boolean.class);

   private static UpdateValueTypeChecker.PropertyTypeResolver resolver() {
      return path -> TYPES.getOrDefault(path[0], Object.class);
   }

   private static UpdateValueTypeChecker.InferredType infer(Object value) {
      return UpdateValueTypeChecker.infer(value, REGISTRY, resolver(), LOG);
   }

   private static void check(Object value, Class<?> targetType) {
      UpdateValueTypeChecker.checkAssignable(value, targetType, "p", REGISTRY, resolver(), LOG);
   }

   // --- Category inference ---

   public void testConstantCategories() {
      assertThat(infer(5L).category()).isEqualTo(UpdateValueTypeChecker.ValueCategory.INTEGER);
      assertThat(infer(5L).foldedValue()).isEqualTo(5L);
      assertThat(infer(3.5).category()).isEqualTo(UpdateValueTypeChecker.ValueCategory.DOUBLE);
      assertThat(infer(3.5).foldedValue()).isEqualTo(3.5);
      assertThat(infer("x").category()).isEqualTo(UpdateValueTypeChecker.ValueCategory.STRING);
      assertThat(infer(true).category()).isEqualTo(UpdateValueTypeChecker.ValueCategory.BOOLEAN);
      assertThat(infer(null).category()).isEqualTo(UpdateValueTypeChecker.ValueCategory.NULL);
   }

   public void testPropertyRefCategories() {
      assertThat(infer(new PropertyRefUpdateValue(new String[]{"rating"})).category())
            .isEqualTo(UpdateValueTypeChecker.ValueCategory.INTEGER);
      assertThat(infer(new PropertyRefUpdateValue(new String[]{"name"})).category())
            .isEqualTo(UpdateValueTypeChecker.ValueCategory.STRING);
      assertThat(infer(new PropertyRefUpdateValue(new String[]{"releaseDate"})).category())
            .isEqualTo(UpdateValueTypeChecker.ValueCategory.TEMPORAL);
      assertThat(infer(new PropertyRefUpdateValue(new String[]{"published"})).category())
            .isEqualTo(UpdateValueTypeChecker.ValueCategory.BOOLEAN);
      assertThat(infer(new PropertyRefUpdateValue(new String[]{"unknown"})).category())
            .isEqualTo(UpdateValueTypeChecker.ValueCategory.UNKNOWN);
   }

   public void testNullPropagation() {
      assertThat(infer(new BinaryArithmeticUpdateValueExpr(ArithmeticOperator.ADD, null, 5L)).category())
            .isEqualTo(UpdateValueTypeChecker.ValueCategory.NULL);
      assertThat(infer(new BinaryArithmeticUpdateValueExpr(ArithmeticOperator.ADD, 5L, null)).category())
            .isEqualTo(UpdateValueTypeChecker.ValueCategory.NULL);
   }

   public void testStringConcatenation() {
      assertThat(infer(new BinaryArithmeticUpdateValueExpr(ArithmeticOperator.ADD, "a", 1L)).category())
            .isEqualTo(UpdateValueTypeChecker.ValueCategory.STRING);
      assertThat(infer(new BinaryArithmeticUpdateValueExpr(ArithmeticOperator.ADD, 1L, "a")).category())
            .isEqualTo(UpdateValueTypeChecker.ValueCategory.STRING);
   }

   public void testArithmeticOnNonNumericRejected() {
      assertThatThrownBy(() -> infer(new BinaryArithmeticUpdateValueExpr(ArithmeticOperator.SUBTRACT, "a", 1L)))
            .isInstanceOf(ParsingException.class)
            .hasMessageContaining("ISPN028538");
      assertThatThrownBy(() -> infer(new NegateUpdateValueExpr("a")))
            .isInstanceOf(ParsingException.class)
            .hasMessageContaining("ISPN028538");
   }

   public void testConstantFolding() {
      UpdateValueTypeChecker.InferredType sum =
            infer(new BinaryArithmeticUpdateValueExpr(ArithmeticOperator.ADD, 3L, 4L));
      assertThat(sum.category()).isEqualTo(UpdateValueTypeChecker.ValueCategory.INTEGER);
      assertThat(sum.foldedValue()).isEqualTo(7L);

      UpdateValueTypeChecker.InferredType mixed =
            infer(new BinaryArithmeticUpdateValueExpr(ArithmeticOperator.ADD, 3.0, 1L));
      assertThat(mixed.category()).isEqualTo(UpdateValueTypeChecker.ValueCategory.DOUBLE);
      assertThat(mixed.foldedValue()).isEqualTo(4.0);

      UpdateValueTypeChecker.InferredType neg = infer(new NegateUpdateValueExpr(5L));
      assertThat(neg.foldedValue()).isEqualTo(-5L);
   }

   public void testConstantDivisionByZeroRejected() {
      assertThatThrownBy(() -> infer(new BinaryArithmeticUpdateValueExpr(ArithmeticOperator.DIVIDE, 1L, 0L)))
            .isInstanceOf(ParsingException.class)
            .hasMessageContaining("ISPN028540");
      assertThatThrownBy(() -> infer(new BinaryArithmeticUpdateValueExpr(ArithmeticOperator.DIVIDE, 1.0, 0.0)))
            .isInstanceOf(ParsingException.class)
            .hasMessageContaining("ISPN028540");
   }

   public void testFunctionDeclaredReturnTypes() {
      // sqrt declares Double, abs declares Number (unknown granularity), upper declares String.
      assertThat(infer(new FunctionCallUpdateValueExpr("sqrt", java.util.List.of(1L))).category())
            .isEqualTo(UpdateValueTypeChecker.ValueCategory.DOUBLE);
      assertThat(infer(new FunctionCallUpdateValueExpr("abs", java.util.List.of(1L))).category())
            .isEqualTo(UpdateValueTypeChecker.ValueCategory.UNKNOWN);
      assertThat(infer(new FunctionCallUpdateValueExpr("upper", java.util.List.of("x"))).category())
            .isEqualTo(UpdateValueTypeChecker.ValueCategory.STRING);
   }

   // --- Assignability ---

   public void testBooleanAssignability() {
      assertThatCode(() -> check(true, String.class)).doesNotThrowAnyException();
      assertThatCode(() -> check(true, boolean.class)).doesNotThrowAnyException();
      assertThatThrownBy(() -> check(true, int.class))
            .isInstanceOf(ParsingException.class)
            .hasMessageContaining("ISPN028537");
      assertThatThrownBy(() -> check(true, LocalDateTime.class))
            .isInstanceOf(ParsingException.class)
            .hasMessageContaining("ISPN028537");
   }

   public void testNumberAssignability() {
      assertThatCode(() -> check(1L, int.class)).doesNotThrowAnyException();
      assertThatCode(() -> check(1L, String.class)).doesNotThrowAnyException();
      assertThatCode(() -> check(3.5, double.class)).doesNotThrowAnyException();
      assertThatThrownBy(() -> check(1L, boolean.class))
            .isInstanceOf(ParsingException.class)
            .hasMessageContaining("ISPN028537");
      assertThatThrownBy(() -> check(1L, LocalDateTime.class))
            .isInstanceOf(ParsingException.class)
            .hasMessageContaining("ISPN028537");
   }

   public void testStringAssignability() {
      // String targets accept anything; string -> temporal is impossible at runtime.
      assertThatCode(() -> check("x", String.class)).doesNotThrowAnyException();
      assertThatCode(() -> check("x", int.class)).doesNotThrowAnyException();
      assertThatThrownBy(() -> check("x", LocalDateTime.class))
            .isInstanceOf(ParsingException.class)
            .hasMessageContaining("ISPN028537");
   }

   public void testNonIntegralFoldedValueRejectedForIntegerTargets() {
      // 7.0 / 2 = 3.5 folded at parse time.
      assertThatThrownBy(() -> check(new BinaryArithmeticUpdateValueExpr(ArithmeticOperator.DIVIDE, 7.0, 2.0), int.class))
            .isInstanceOf(ParsingException.class)
            .hasMessageContaining("ISPN028539");
      // Integral folded values convert normally.
      assertThatCode(() -> check(3.0, int.class)).doesNotThrowAnyException();
      assertThatCode(() -> check(new BinaryArithmeticUpdateValueExpr(ArithmeticOperator.DIVIDE, 7L, 2L), int.class))
            .doesNotThrowAnyException();
      // Non-integral values are fine for floating point targets.
      assertThatCode(() -> check(new BinaryArithmeticUpdateValueExpr(ArithmeticOperator.DIVIDE, 7.0, 2.0), double.class))
            .doesNotThrowAnyException();
   }

   public void testTemporalAssignability() {
      assertThatCode(() -> check(LocalDateTime.now(), LocalDateTime.class)).doesNotThrowAnyException();
      assertThatCode(() -> check(LocalDateTime.now(), String.class)).doesNotThrowAnyException();
      assertThatThrownBy(() -> check(LocalDateTime.now(), int.class))
            .isInstanceOf(ParsingException.class)
            .hasMessageContaining("ISPN028537");
   }
}
