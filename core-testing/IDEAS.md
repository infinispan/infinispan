# Testing Module Ideas

## NonBlockingStore test suite: interface contracts vs abstract class

The current `AbstractNonBlockingStoreTest` uses a single-level abstract class, which is
pragmatic and matches established patterns.

An alternative is **JUnit 5 interface contracts with default test methods**:

```java
interface NonBlockingStoreContract {
    NonBlockingStore<Object, Object> store();
    ControlledTimeService timeService();
    // ... other accessors

    @Test
    default void testLoadAndStoreImmortal() {
        // test using store(), entryFactory(), etc.
    }
}
```

Benefits:
- No class hierarchy — implementing class owns its setup
- A store can implement **multiple contracts** (e.g., `NonBlockingStoreContract` +
  `TransactionalStoreContract`) which is impossible with single inheritance
- Clearer separation between "what to test" and "how to set up"

Trade-offs:
- More boilerplate in implementing classes (accessor methods, setup logic)
- Setup logic could be extracted into a reusable extension or helper

Consider refactoring if stores ever need to combine multiple test contracts.
