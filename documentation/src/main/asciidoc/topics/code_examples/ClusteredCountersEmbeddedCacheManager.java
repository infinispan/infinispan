CounterManager manager = ...;
manager.defineCounter("c1", CounterConfiguration.builder(CounterType.UNBOUNDED_STRONG).initialValue(1).storage(Storage.PERSISTENT).build());
manager.defineCounter("c2", CounterConfiguration.builder(CounterType.BOUNDED_STRONG).initialValue(2).lowerBound(0).storage(Storage.VOLATILE).build());
manager.defineCounter("c3", CounterConfiguration.builder(CounterType.BOUNDED_STRONG).initialValue(3).upperBound(5).storage(Storage.PERSISTENT).build());
manager.defineCounter("c4", CounterConfiguration.builder(CounterType.BOUNDED_STRONG).initialValue(4).lowerBound(0).upperBound(10).storage(Storage.VOLATILE).build());
manager.defineCounter("c4", CounterConfiguration.builder(CounterType.BOUNDED_STRONG).initialValue(0).upperBound(100).lifespan(60000).build());
manager.defineCounter("c6", CounterConfiguration.builder(CounterType.WEAK).initialValue(5).concurrencyLevel(1).storage(Storage.PERSISTENT).build());
