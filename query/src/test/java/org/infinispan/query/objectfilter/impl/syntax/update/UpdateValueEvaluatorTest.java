package org.infinispan.query.objectfilter.impl.syntax.update;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.infinispan.query.functions.QueryFunctionContext;
import org.infinispan.query.objectfilter.impl.syntax.ConstantValueExpr;
import org.testng.annotations.Test;

@Test(testName = "query.objectfilter.impl.syntax.update.UpdateValueEvaluatorTest", groups = "functional")
public class UpdateValueEvaluatorTest {

   private QueryFunctionContext ctx(Object entity, Map<String, Object> params) {
      return new QueryFunctionContext(entity,
            path -> path.length == 1 ? "prop-" + path[0] : "nested",
            params,
            QueryFunctionRegistry.standard());
   }

   // --- Arithmetic ---

   public void testIntegerAddition() {
      Object result = UpdateValueEvaluator.applyArithmetic(ArithmeticOperator.ADD, 3L, 5L);
      assertThat(result).isEqualTo(8L);
   }

   public void testIntegerSubtraction() {
      Object result = UpdateValueEvaluator.applyArithmetic(ArithmeticOperator.SUBTRACT, 10L, 4L);
      assertThat(result).isEqualTo(6L);
   }

   public void testIntegerMultiplication() {
      Object result = UpdateValueEvaluator.applyArithmetic(ArithmeticOperator.MULTIPLY, 3L, 7L);
      assertThat(result).isEqualTo(21L);
   }

   public void testIntegerDivision() {
      Object result = UpdateValueEvaluator.applyArithmetic(ArithmeticOperator.DIVIDE, 10L, 3L);
      assertThat(result).isEqualTo(3L);
   }

   public void testIntegerModulo() {
      Object result = UpdateValueEvaluator.applyArithmetic(ArithmeticOperator.MODULO, 10L, 3L);
      assertThat(result).isEqualTo(1L);
   }

   public void testDoubleAddition() {
      Object result = UpdateValueEvaluator.applyArithmetic(ArithmeticOperator.ADD, 1.5, 2.5);
      assertThat(result).isEqualTo(4.0);
   }

   public void testMixedIntegerDouble() {
      Object result = UpdateValueEvaluator.applyArithmetic(ArithmeticOperator.ADD, 1L, 2.5);
      assertThat(result).isEqualTo(3.5);
   }

   public void testStringConcatenation() {
      Object result = UpdateValueEvaluator.applyArithmetic(ArithmeticOperator.ADD, "hello", " world");
      assertThat(result).isEqualTo("hello world");
   }

   public void testNullPropagates() {
      assertThat(UpdateValueEvaluator.applyArithmetic(ArithmeticOperator.ADD, null, 5L)).isNull();
      assertThat(UpdateValueEvaluator.applyArithmetic(ArithmeticOperator.ADD, 5L, null)).isNull();
   }

   public void testDivisionByZero() {
      assertThatThrownBy(() -> UpdateValueEvaluator.applyArithmetic(ArithmeticOperator.DIVIDE, 1L, 0L))
            .isInstanceOf(ArithmeticException.class);
   }

   public void testModuloByZero() {
      assertThatThrownBy(() -> UpdateValueEvaluator.applyArithmetic(ArithmeticOperator.MODULO, 1L, 0L))
            .isInstanceOf(ArithmeticException.class);
   }

   public void testNegate() {
      assertThat(UpdateValueEvaluator.applyNegate(5L)).isEqualTo(-5L);
      assertThat(UpdateValueEvaluator.applyNegate(3.14)).isEqualTo(-3.14);
      assertThat(UpdateValueEvaluator.applyNegate(null)).isNull();
   }

   // --- Binary expression tree ---

   public void testBinaryExpression() {
      BinaryArithmeticUpdateValueExpr expr = new BinaryArithmeticUpdateValueExpr(
            ArithmeticOperator.ADD, 3L, 7L);
      Object result = expr.evaluate(ctx(null, Map.of()));
      assertThat(result).isEqualTo(10L);
   }

   public void testNestedBinaryExpression() {
      // (2 + 3) * 4 = 20
      BinaryArithmeticUpdateValueExpr inner = new BinaryArithmeticUpdateValueExpr(
            ArithmeticOperator.ADD, 2L, 3L);
      BinaryArithmeticUpdateValueExpr outer = new BinaryArithmeticUpdateValueExpr(
            ArithmeticOperator.MULTIPLY, inner, 4L);
      Object result = outer.evaluate(ctx(null, Map.of()));
      assertThat(result).isEqualTo(20L);
   }

   public void testPrecedenceMultiplicationBeforeAddition() {
      // 2 + 3 * 4 = 14 (built as ADD(2, MUL(3,4)))
      BinaryArithmeticUpdateValueExpr mul = new BinaryArithmeticUpdateValueExpr(
            ArithmeticOperator.MULTIPLY, 3L, 4L);
      BinaryArithmeticUpdateValueExpr add = new BinaryArithmeticUpdateValueExpr(
            ArithmeticOperator.ADD, 2L, mul);
      Object result = add.evaluate(ctx(null, Map.of()));
      assertThat(result).isEqualTo(14L);
   }

   // --- Property reference ---

   public void testPropertyRef() {
      PropertyRefUpdateValue ref = new PropertyRefUpdateValue(new String[]{"age"});
      Object result = ref.evaluate(ctx(null, Map.of()));
      assertThat(result).isEqualTo("prop-age");
   }

   public void testPropertyRefInArithmetic() {
      // age + 1 (propertyReader returns "prop-age" String, so this tests string concat)
      PropertyRefUpdateValue ref = new PropertyRefUpdateValue(new String[]{"age"});
      BinaryArithmeticUpdateValueExpr expr = new BinaryArithmeticUpdateValueExpr(
            ArithmeticOperator.ADD, ref, 1L);
      Object result = expr.evaluate(ctx(null, Map.of()));
      assertThat(result).isEqualTo("prop-age1");
   }

   // --- Function calls ---

   public void testFunctionAbs() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("abs", List.of(-5L));
      Object result = expr.evaluate(ctx(null, Map.of()));
      assertThat(result).isEqualTo(5L);
   }

   public void testFunctionUpper() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("upper", List.of("hello"));
      Object result = expr.evaluate(ctx(null, Map.of()));
      assertThat(result).isEqualTo("HELLO");
   }

   public void testFunctionConcat() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("concat",
            List.of("foo", "bar"));
      Object result = expr.evaluate(ctx(null, Map.of()));
      assertThat(result).isEqualTo("foobar");
   }

   public void testFunctionMin() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("min", List.of(3L, 7L));
      Object result = expr.evaluate(ctx(null, Map.of()));
      assertThat(result).isEqualTo(3L);
   }

   public void testFunctionInExpression() {
      // abs(-5) + 3 = 8
      FunctionCallUpdateValueExpr absExpr = new FunctionCallUpdateValueExpr("abs", List.of(-5L));
      BinaryArithmeticUpdateValueExpr addExpr = new BinaryArithmeticUpdateValueExpr(
            ArithmeticOperator.ADD, absExpr, 3L);
      Object result = addExpr.evaluate(ctx(null, Map.of()));
      assertThat(result).isEqualTo(8L);
   }

   // --- Parameters ---

   public void testParameterResolution() {
      ConstantValueExpr.ParamPlaceholder param = new ConstantValueExpr.ParamPlaceholder("inc");
      Map<String, Object> params = Map.of("inc", 10L);
      Object result = UpdateValueEvaluator.evaluateOperand(param, ctx(null, params));
      assertThat(result).isEqualTo(10L);
   }

   public void testMissingParameter() {
      ConstantValueExpr.ParamPlaceholder param = new ConstantValueExpr.ParamPlaceholder("missing");
      assertThatThrownBy(() -> UpdateValueEvaluator.evaluateOperand(param, ctx(null, Map.of())))
            .isInstanceOf(IllegalArgumentException.class);
   }

   // --- Negate expression ---

   public void testNegateExpression() {
      NegateUpdateValueExpr expr = new NegateUpdateValueExpr(5L);
      Object result = expr.evaluate(ctx(null, Map.of()));
      assertThat(result).isEqualTo(-5L);
   }

   public void testNegateDouble() {
      NegateUpdateValueExpr expr = new NegateUpdateValueExpr(3.14);
      Object result = expr.evaluate(ctx(null, Map.of()));
      assertThat(result).isEqualTo(-3.14);
   }

   // --- Registry ---

   public void testRegistryContains() {
      QueryFunctionRegistry reg = QueryFunctionRegistry.standard();
      assertThat(reg.contains("abs")).isTrue();
      assertThat(reg.contains("ABS")).isTrue();
      assertThat(reg.contains("nonexistent")).isFalse();
   }

   public void testRegistryCustomFunction() {
      QueryFunctionRegistry reg = QueryFunctionRegistry.standard();
      reg.add("double", (args, c) -> {
         Object v = args.get(0);
         if (v instanceof Number n) {
            return n.longValue() * 2;
         }
         return null;
      });
      assertThat(reg.contains("double")).isTrue();
      QueryFunctionContext customCtx = new QueryFunctionContext(null,
            path -> path.length == 1 ? "prop-" + path[0] : "nested",
            Map.of(),
            reg);
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("double", List.of(21L));
      Object result = expr.evaluate(customCtx);
      assertThat(result).isEqualTo(42L);
   }

   public void testRegistryInstancesAreIndependent() {
      QueryFunctionRegistry first = QueryFunctionRegistry.standard();
      QueryFunctionRegistry second = QueryFunctionRegistry.standard();
      first.add("onlyinfirst", (args, c) -> 1L);
      assertThat(first.contains("onlyinfirst")).isTrue();
      assertThat(second.contains("onlyinfirst")).isFalse();
      assertThat(QueryFunctionRegistry.standard().contains("onlyinfirst")).isFalse();
      first.remove("abs");
      assertThat(first.contains("abs")).isFalse();
      assertThat(second.contains("abs")).isTrue();
      assertThat(second.getDeclaredReturnType("abs")).isNotNull();
   }

   public void testRegistryNames() {
      QueryFunctionRegistry reg = QueryFunctionRegistry.standard();
      assertThat(reg.names()).contains("abs", "round", "floor", "ceil", "min", "max",
            "power", "sqrt", "concat", "upper", "lower", "trim", "length", "substring",
            "replace", "adddays", "addhours", "addminutes", "addmonths", "addyears",
            "dateadd", "tolong", "todouble", "tostring");
   }

   // --- Math function coverage ---

   public void testFunctionAbsDouble() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("abs", List.of(-5.5));
      assertThat(expr.evaluate(ctx(null, Map.of()))).isEqualTo(5.5);
   }

   public void testFunctionAbsNull() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("abs", Collections.singletonList(null));
      assertThat(expr.evaluate(ctx(null, Map.of()))).isNull();
   }

   public void testFunctionRound() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("round", List.of(3.7));
      assertThat(expr.evaluate(ctx(null, Map.of()))).isEqualTo(4L);
   }

   public void testFunctionRoundNull() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("round", Collections.singletonList(null));
      assertThat(expr.evaluate(ctx(null, Map.of()))).isNull();
   }

   public void testFunctionFloorDouble() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("floor", List.of(3.7));
      assertThat(expr.evaluate(ctx(null, Map.of()))).isEqualTo(3.0);
   }

   public void testFunctionFloorIntegral() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("floor", List.of(5L));
      assertThat(expr.evaluate(ctx(null, Map.of()))).isEqualTo(5L);
   }

   public void testFunctionFloorNull() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("floor", Collections.singletonList(null));
      assertThat(expr.evaluate(ctx(null, Map.of()))).isNull();
   }

   public void testFunctionCeilDouble() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("ceil", List.of(3.2));
      assertThat(expr.evaluate(ctx(null, Map.of()))).isEqualTo(4.0);
   }

   public void testFunctionCeilIntegral() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("ceil", List.of(5L));
      assertThat(expr.evaluate(ctx(null, Map.of()))).isEqualTo(5L);
   }

   public void testFunctionCeilNull() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("ceil", Collections.singletonList(null));
      assertThat(expr.evaluate(ctx(null, Map.of()))).isNull();
   }

   public void testFunctionMax() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("max", List.of(3L, 7L));
      assertThat(expr.evaluate(ctx(null, Map.of()))).isEqualTo(7L);
   }

   public void testFunctionMaxDouble() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("max", List.of(3.5, 7.2));
      assertThat(expr.evaluate(ctx(null, Map.of()))).isEqualTo(7.2);
   }

   public void testFunctionMaxNull() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("max", Arrays.asList(null, 7L));
      assertThat(expr.evaluate(ctx(null, Map.of()))).isNull();
   }

   public void testFunctionMaxNonNumeric() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("max", List.of("a", 7L));
      assertThatThrownBy(() -> expr.evaluate(ctx(null, Map.of())))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("numeric");
   }

   public void testFunctionPower() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("power", List.of(2L, 10L));
      assertThat(expr.evaluate(ctx(null, Map.of()))).isEqualTo(1024L);
   }

   public void testFunctionPowerDouble() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("power", List.of(2.5, 2.0));
      assertThat(expr.evaluate(ctx(null, Map.of()))).isEqualTo(6.25);
   }

   public void testFunctionPowerNull() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("power", Arrays.asList(null, 2L));
      assertThat(expr.evaluate(ctx(null, Map.of()))).isNull();
   }

   public void testFunctionPowerNonNumeric() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("power", List.of("a", 2L));
      assertThatThrownBy(() -> expr.evaluate(ctx(null, Map.of())))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("numeric");
   }

   public void testFunctionSqrt() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("sqrt", List.of(16.0));
      assertThat(expr.evaluate(ctx(null, Map.of()))).isEqualTo(4.0);
   }

   public void testFunctionSqrtNull() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("sqrt", Collections.singletonList(null));
      assertThat(expr.evaluate(ctx(null, Map.of()))).isNull();
   }

   // --- String function coverage ---

   public void testFunctionLower() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("lower", List.of("HELLO"));
      assertThat(expr.evaluate(ctx(null, Map.of()))).isEqualTo("hello");
   }

   public void testFunctionLowerNull() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("lower", Collections.singletonList(null));
      assertThat(expr.evaluate(ctx(null, Map.of()))).isNull();
   }

   public void testFunctionTrim() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("trim", List.of("  padded  "));
      assertThat(expr.evaluate(ctx(null, Map.of()))).isEqualTo("padded");
   }

   public void testFunctionTrimNull() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("trim", Collections.singletonList(null));
      assertThat(expr.evaluate(ctx(null, Map.of()))).isNull();
   }

   public void testFunctionLength() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("length", List.of("hello"));
      assertThat(expr.evaluate(ctx(null, Map.of()))).isEqualTo(5);
   }

   public void testFunctionLengthNull() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("length", Collections.singletonList(null));
      assertThat(expr.evaluate(ctx(null, Map.of()))).isNull();
   }

   public void testFunctionSubstring() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("substring", List.of("Hello", 2L, 4L));
      assertThat(expr.evaluate(ctx(null, Map.of()))).isEqualTo("el");
   }

   public void testFunctionSubstringTwoArgs() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("substring", List.of("Hello", 2L));
      assertThat(expr.evaluate(ctx(null, Map.of()))).isEqualTo("ello");
   }

   public void testFunctionSubstringNull() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("substring", Arrays.asList(null, 1L));
      assertThat(expr.evaluate(ctx(null, Map.of()))).isNull();
   }

   public void testFunctionSubstringInvalidRange() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("substring", List.of("Hi", 5L, 10L));
      assertThatThrownBy(() -> expr.evaluate(ctx(null, Map.of())))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("invalid range");
   }

   public void testFunctionReplace() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("replace", List.of("aaa", "a", "b"));
      assertThat(expr.evaluate(ctx(null, Map.of()))).isEqualTo("bbb");
   }

   public void testFunctionReplaceNull() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("replace", Arrays.asList(null, "a", "b"));
      assertThat(expr.evaluate(ctx(null, Map.of()))).isNull();
   }

   public void testFunctionConcatNull() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("concat",
            Arrays.asList("a", null, "b"));
      assertThat(expr.evaluate(ctx(null, Map.of()))).isNull();
   }

   // --- Date function coverage ---

   public void testFunctionAddDays() {
      Date base = new Date(0);
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("addDays", List.of(base, 7L));
      Object result = expr.evaluate(ctx(null, Map.of()));
      assertThat(result).isInstanceOf(Date.class);
      assertThat(((Date) result).getTime()).isEqualTo(7L * 24 * 3600 * 1000);
   }

   public void testFunctionAddHours() {
      Date base = new Date(0);
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("addHours", List.of(base, -24L));
      Object result = expr.evaluate(ctx(null, Map.of()));
      assertThat(((Date) result).getTime()).isEqualTo(-24L * 3600 * 1000);
   }

   public void testFunctionAddMinutes() {
      Date base = new Date(0);
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("addMinutes", List.of(base, 30L));
      Object result = expr.evaluate(ctx(null, Map.of()));
      assertThat(((Date) result).getTime()).isEqualTo(30L * 60 * 1000);
   }

   public void testFunctionAddMonths() {
      Date base = new Date(0);
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("addMonths", List.of(base, 1L));
      Object result = expr.evaluate(ctx(null, Map.of()));
      assertThat(result).isInstanceOf(Date.class);
      assertThat(((Date) result).getTime()).isGreaterThan(base.getTime());
   }

   public void testFunctionAddYears() {
      Date base = new Date(0);
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("addYears", List.of(base, 1L));
      Object result = expr.evaluate(ctx(null, Map.of()));
      assertThat(result).isInstanceOf(Date.class);
      assertThat(((Date) result).getTime()).isGreaterThan(base.getTime());
   }

   public void testFunctionAddDaysNull() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("addDays", Arrays.asList(null, 7L));
      assertThat(expr.evaluate(ctx(null, Map.of()))).isNull();
   }

   public void testFunctionAddDaysWrongType() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("addDays", List.of("not-a-date", 7L));
      assertThatThrownBy(() -> expr.evaluate(ctx(null, Map.of())))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("expects a date/time argument");
   }

   public void testFunctionAddDaysLocalDateTime() {
      LocalDateTime base = LocalDateTime.of(2020, 1, 31, 12, 0);
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("addDays", List.of(base, 7L));
      Object result = expr.evaluate(ctx(null, Map.of()));
      assertThat(result).isInstanceOf(LocalDateTime.class);
      assertThat(result).isEqualTo(LocalDateTime.of(2020, 2, 7, 12, 0));
   }

   public void testFunctionAddMonthsLocalDate() {
      LocalDate base = LocalDate.of(2020, 1, 31);
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("addMonths", List.of(base, 1L));
      Object result = expr.evaluate(ctx(null, Map.of()));
      assertThat(result).isInstanceOf(LocalDate.class);
      assertThat(result).isEqualTo(LocalDate.of(2020, 2, 29)); // 2020 is a leap year
   }

   public void testFunctionAddDaysInstant() {
      Instant base = Instant.ofEpochSecond(0);
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("addDays", List.of(base, 7L));
      Object result = expr.evaluate(ctx(null, Map.of()));
      assertThat(result).isInstanceOf(Instant.class);
      assertThat(result).isEqualTo(Instant.ofEpochSecond(7L * 24 * 3600));
   }

   public void testFunctionAddDaysCalendar() {
      Calendar base = Calendar.getInstance();
      base.clear();
      base.set(2020, Calendar.JANUARY, 31);
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("addDays", List.of(base, 7L));
      Object result = expr.evaluate(ctx(null, Map.of()));
      assertThat(result).isInstanceOf(Calendar.class);
      Calendar r = (Calendar) result;
      assertThat(r.get(Calendar.YEAR)).isEqualTo(2020);
      assertThat(r.get(Calendar.MONTH)).isEqualTo(Calendar.FEBRUARY);
      assertThat(r.get(Calendar.DAY_OF_MONTH)).isEqualTo(7);
   }

   public void testFunctionAddDaysZonedDateTime() {
      ZonedDateTime base = ZonedDateTime.of(2020, 1, 31, 12, 0, 0, 0, ZoneOffset.UTC);
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("addDays", List.of(base, 7L));
      Object result = expr.evaluate(ctx(null, Map.of()));
      assertThat(result).isInstanceOf(ZonedDateTime.class);
      assertThat(result).isEqualTo(ZonedDateTime.of(2020, 2, 7, 12, 0, 0, 0, ZoneOffset.UTC));
   }

   public void testFunctionDateAdd() {
      Date base = new Date(0);
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("dateAdd", List.of(base, 3L, "DAY"));
      Object result = expr.evaluate(ctx(null, Map.of()));
      assertThat(((Date) result).getTime()).isEqualTo(3L * 24 * 3600 * 1000);
   }

   public void testFunctionDateAddWeek() {
      Date base = new Date(0);
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("dateAdd", List.of(base, 1L, "WEEK"));
      Object result = expr.evaluate(ctx(null, Map.of()));
      assertThat(((Date) result).getTime()).isEqualTo(7L * 24 * 3600 * 1000);
   }

   public void testFunctionDateAddNull() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("dateAdd", Arrays.asList(null, 3L, "DAY"));
      assertThat(expr.evaluate(ctx(null, Map.of()))).isNull();
   }

   public void testFunctionDateAddUnknownUnit() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("dateAdd", List.of(new Date(0), 3L, "INVALID"));
      assertThatThrownBy(() -> expr.evaluate(ctx(null, Map.of())))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("unknown unit");
   }

   // --- Conversion function coverage ---

   public void testFunctionToLongFromString() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("toLong", List.of("123"));
      assertThat(expr.evaluate(ctx(null, Map.of()))).isEqualTo(123L);
   }

   public void testFunctionToLongFromNumber() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("toLong", List.of(42.9));
      assertThat(expr.evaluate(ctx(null, Map.of()))).isEqualTo(42L);
   }

   public void testFunctionToLongInvalidString() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("toLong", List.of("abc"));
      assertThatThrownBy(() -> expr.evaluate(ctx(null, Map.of())))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("cannot parse");
   }

   public void testFunctionToLongWrongType() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("toLong", List.of(true));
      assertThatThrownBy(() -> expr.evaluate(ctx(null, Map.of())))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("cannot convert");
   }

   public void testFunctionToLongNull() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("toLong", Collections.singletonList(null));
      assertThat(expr.evaluate(ctx(null, Map.of()))).isNull();
   }

   public void testFunctionToDoubleFromString() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("toDouble", List.of("3.14"));
      assertThat(expr.evaluate(ctx(null, Map.of()))).isEqualTo(3.14);
   }

   public void testFunctionToDoubleFromNumber() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("toDouble", List.of(42L));
      assertThat(expr.evaluate(ctx(null, Map.of()))).isEqualTo(42.0);
   }

   public void testFunctionToDoubleInvalidString() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("toDouble", List.of("abc"));
      assertThatThrownBy(() -> expr.evaluate(ctx(null, Map.of())))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("cannot parse");
   }

   public void testFunctionToDoubleWrongType() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("toDouble", List.of(true));
      assertThatThrownBy(() -> expr.evaluate(ctx(null, Map.of())))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("cannot convert");
   }

   public void testFunctionToDoubleNull() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("toDouble", Collections.singletonList(null));
      assertThat(expr.evaluate(ctx(null, Map.of()))).isNull();
   }

   public void testFunctionToString() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("toString", List.of(42L));
      assertThat(expr.evaluate(ctx(null, Map.of()))).isEqualTo("42");
   }

   public void testFunctionToStringNull() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("toString", Collections.singletonList(null));
      assertThat(expr.evaluate(ctx(null, Map.of()))).isNull();
   }

   // --- Arithmetic error and null propagation ---

   public void testArithmeticDivisionByZero() {
      assertThatThrownBy(() -> UpdateValueEvaluator.applyArithmetic(ArithmeticOperator.DIVIDE, 10L, 0L))
            .isInstanceOf(ArithmeticException.class)
            .hasMessageContaining("Division by zero");
   }

   public void testArithmeticDoubleDivisionByZero() {
      assertThatThrownBy(() -> UpdateValueEvaluator.applyArithmetic(ArithmeticOperator.DIVIDE, 10.0, 0.0))
            .isInstanceOf(ArithmeticException.class)
            .hasMessageContaining("Division by zero");
   }

   public void testArithmeticModuloByZero() {
      assertThatThrownBy(() -> UpdateValueEvaluator.applyArithmetic(ArithmeticOperator.MODULO, 10L, 0L))
            .isInstanceOf(ArithmeticException.class)
            .hasMessageContaining("Division by zero");
   }

   public void testArithmeticNullLeft() {
      assertThat(UpdateValueEvaluator.applyArithmetic(ArithmeticOperator.ADD, null, 1L)).isNull();
   }

   public void testArithmeticNullRight() {
      assertThat(UpdateValueEvaluator.applyArithmetic(ArithmeticOperator.MULTIPLY, 5L, null)).isNull();
   }

   public void testArithmeticNonNumeric() {
      assertThatThrownBy(() -> UpdateValueEvaluator.applyArithmetic(ArithmeticOperator.MULTIPLY, "hello", 3L))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Cannot apply operator");
   }

   public void testNegateNonNumeric() {
      assertThatThrownBy(() -> UpdateValueEvaluator.applyNegate("hello"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Cannot negate");
   }

   public void testNegateNull() {
      assertThat(UpdateValueEvaluator.applyNegate(null)).isNull();
   }

   // --- Arity errors ---

   public void testFunctionWrongArity() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("abs", List.of(1L, 2L));
      assertThatThrownBy(() -> expr.evaluate(ctx(null, Map.of())))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("expects 1 argument");
   }

   public void testFunctionConcatWrongArity() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("concat", List.of());
      assertThatThrownBy(() -> expr.evaluate(ctx(null, Map.of())))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("expects between");
   }

   // --- singleNumber error ---

   public void testFunctionAbsNonNumeric() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("abs", List.of("hello"));
      assertThatThrownBy(() -> expr.evaluate(ctx(null, Map.of())))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("expects a numeric argument");
   }

   // --- Regexp ---

   public void testRegexpLikeTrue() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("regexp_like", List.of("hello123", "\\d+"));
      assertThat(expr.evaluate(ctx(null, Map.of()))).isEqualTo(true);
   }

   public void testRegexpLikeFalse() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("regexp_like", List.of("hello", "\\d+"));
      assertThat(expr.evaluate(ctx(null, Map.of()))).isEqualTo(false);
   }

   public void testRegexpLikeNull() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("regexp_like", Arrays.asList(null, "\\d+"));
      assertThat(expr.evaluate(ctx(null, Map.of()))).isNull();
   }

   public void testRegexpLikeNullPattern() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("regexp_like", Arrays.asList("hello", null));
      assertThat(expr.evaluate(ctx(null, Map.of()))).isNull();
   }

   public void testRegexpReplace() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("regexp_replace", List.of("hello123world", "\\d+", "X"));
      assertThat(expr.evaluate(ctx(null, Map.of()))).isEqualTo("helloXworld");
   }

   public void testRegexpReplaceMultiple() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("regexp_replace", List.of("a1b22c333", "\\d", "#"));
      assertThat(expr.evaluate(ctx(null, Map.of()))).isEqualTo("a#b##c###");
   }

   public void testRegexpReplaceGroups() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("regexp_replace", List.of("a1b22c333", "\\d+", "#"));
      assertThat(expr.evaluate(ctx(null, Map.of()))).isEqualTo("a#b#c#");
   }

   public void testRegexpReplaceNull() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("regexp_replace", Arrays.asList(null, "\\d+", "X"));
      assertThat(expr.evaluate(ctx(null, Map.of()))).isNull();
   }

   public void testRegexpReplaceNullPattern() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("regexp_replace", Arrays.asList("hello", null, "X"));
      assertThat(expr.evaluate(ctx(null, Map.of()))).isNull();
   }

   public void testRegexpSubstr() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("regexp_substr", List.of("hello123world", "\\d+"));
      assertThat(expr.evaluate(ctx(null, Map.of()))).isEqualTo("123");
   }

   public void testRegexpSubstrNoMatch() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("regexp_substr", List.of("hello", "\\d+"));
      assertThat(expr.evaluate(ctx(null, Map.of()))).isNull();
   }

   public void testRegexpSubstrNull() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("regexp_substr", Arrays.asList(null, "\\d+"));
      assertThat(expr.evaluate(ctx(null, Map.of()))).isNull();
   }

   public void testRegexpInstr() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("regexp_instr", List.of("hello123", "\\d+"));
      assertThat(expr.evaluate(ctx(null, Map.of()))).isEqualTo(6);
   }

   public void testRegexpInstrNoMatch() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("regexp_instr", List.of("hello", "\\d+"));
      assertThat(expr.evaluate(ctx(null, Map.of()))).isEqualTo(0);
   }

   public void testRegexpInstrNull() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("regexp_instr", Arrays.asList(null, "\\d+"));
      assertThat(expr.evaluate(ctx(null, Map.of()))).isNull();
   }

   public void testRegexpLikeInvalidPattern() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("regexp_like", List.of("hello", "[invalid"));
      assertThatThrownBy(() -> expr.evaluate(ctx(null, Map.of())))
            .isInstanceOf(java.util.regex.PatternSyntaxException.class);
   }

   public void testRegexpPreparePrecompiles() {
      java.util.Map<String, java.util.regex.Pattern> cache = new java.util.concurrent.ConcurrentHashMap<>();
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("regexp_like", List.of("hello", "\\d+"));
      expr.prepare(QueryFunctionRegistry.standard(), cache);
      assertThat(cache).containsKey("\\d+");
   }

   public void testRegexpPrepareSkipsNonLiteral() {
      java.util.Map<String, java.util.regex.Pattern> cache = new java.util.concurrent.ConcurrentHashMap<>();
      PropertyRefUpdateValue propRef = new PropertyRefUpdateValue(new String[]{"pattern"});
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("regexp_like", List.of(propRef, propRef));
      expr.prepare(QueryFunctionRegistry.standard(), cache);
      assertThat(cache).isEmpty();
   }

   public void testRegexpPrepareSkipsNonRegexpFunction() {
      java.util.Map<String, java.util.regex.Pattern> cache = new java.util.concurrent.ConcurrentHashMap<>();
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("upper", List.of("hello"));
      expr.prepare(QueryFunctionRegistry.standard(), cache);
      assertThat(cache).isEmpty();
   }

   // --- Function resolution during prepare ---

   public void testEvaluateUsesFunctionResolvedDuringPrepare() {
      QueryFunctionRegistry preparedAgainst = QueryFunctionRegistry.standard();
      preparedAgainst.add("negativeTen", (args, c) -> -10L);
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("negativeTen", List.of());
      expr.prepare(preparedAgainst, new java.util.concurrent.ConcurrentHashMap<>());
      // Evaluate against a registry that does not contain the function: only the
      // reference cached during prepare can supply it.
      QueryFunctionContext ctx = new QueryFunctionContext(null,
            path -> path.length == 1 ? "prop-" + path[0] : "nested",
            Map.of(),
            QueryFunctionRegistry.standard());
      assertThat(expr.evaluate(ctx)).isEqualTo(-10L);
   }

   public void testUnpreparedEvaluateStillLooksUpInRegistry() {
      FunctionCallUpdateValueExpr expr = new FunctionCallUpdateValueExpr("abs", List.of(-5L));
      QueryFunctionContext ctx = new QueryFunctionContext(null,
            path -> path.length == 1 ? "prop-" + path[0] : "nested",
            Map.of(),
            QueryFunctionRegistry.standard());
      assertThat(expr.evaluate(ctx)).isEqualTo(5L);
   }

   // --- Diagnostic query string output ---

   public void testConstantUpdateValueQueryString() {
      StringBuilder sb = new StringBuilder();
      new ConstantUpdateValue(null).appendQueryString(sb);
      assertThat(sb.toString()).isEqualTo("null");
      sb.setLength(0);
      new ConstantUpdateValue(42L).appendQueryString(sb);
      assertThat(sb.toString()).isEqualTo("42");
      sb.setLength(0);
      new ConstantUpdateValue("it's").appendQueryString(sb);
      // Embedded single quotes are doubled, keeping the output valid Ickle
      assertThat(sb.toString()).isEqualTo("'it''s'");
   }
}
