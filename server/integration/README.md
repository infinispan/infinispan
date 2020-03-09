# Vocabulary
* Client side: Code executed outside the container classpath
* Container side: Code executed inside the container
* Container: Wildfly, Tomcat, ... 

# Why not org.junit.rules.TestRule

If we have `FooTestRule`

```java
public class FooTestRule implements TestRule {
   @Override
   public Statement apply(Statement base, Description description) {
      return new Statement() {
         @Override
         public void evaluate() throws Throwable {
            base.evaluate();
         }
      };
   }
}
```

The `evaluate()` method will be called twice: client and container side.

## Fix

`ArquillianSupport` has a method call to: `Class.forName("org.infinispan.server.integration.InstrumentArquillianContainer");`
`InstrumentArquillianContainer` won't be added inside the war and a `ClassNotFoundException` will be raised. 
It means that the code will be executed only in the client side.

### DRY
Also, for each test we will need to repeat:
```java
   @ClassRule
   public static FooTestRule fooTestRule = new FooTestRule();
```
`InfinispanServerIntegrationProcessorExecuter` is a `observer` and is declared once.