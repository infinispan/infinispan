package org.infinispan.lock;

import java.lang.annotation.Annotation;
import java.util.concurrent.Callable;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.provider.MethodSource;

public class SampleTest  {

   @TestFactory
   static class SampleArgumentsSource implements ArgumentsProvider {
      @Override
      public Stream<? extends Arguments> provideArguments(ExtensionContext context) throws Exception {
         System.out.println("provideArguments");
         return Stream.of((Supplier<String>)() -> "1", () -> "2").map(Supplier::get).peek(s -> System.out.println("peek" + s)).map(Arguments::of);
      }
   }

   static class Test extends AbstractTest {
      @ParameterizedTest
//      @ArgumentsSource(SampleArgumentsSource.class)
      @MethodSource("args")
      public void test(String s) {
         System.out.println(s);
      }

      @ParameterizedTest
//      @ArgumentsSource(SampleArgumentsSource.class)
      @MethodSource("args")
      public void test2(String s) {
         System.out.println(s);
      }

      static Stream<Arguments> args() {
         return Stream.of("5").map(Arguments::of);
//         return Stream.of((Supplier<String>)() -> "3", () -> "4").map(Supplier::get).peek(s -> System.out.println("peek" + s)).map(Arguments::of);
      }

   }
   static class AbstractTest {
      static Stream<Arguments> args() {
         return Stream.of((Supplier<String>)() -> "1", () -> "2").map(Supplier::get).peek(s -> System.out.println("peek" + s)).map(Arguments::of);
      }
   }
}
