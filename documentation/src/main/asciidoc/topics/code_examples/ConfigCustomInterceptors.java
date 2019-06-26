Configuration config = new ConfigurationBuilder()
  .customInterceptors().addInterceptor()
    .interceptor(new FirstInterceptor()).position(InterceptorConfiguration.Position.FIRST)
    .interceptor(new LastInterceptor()).position(InterceptorConfiguration.Position.LAST)
    .interceptor(new FixPositionInterceptor()).index(8)
    .interceptor(new AfterInterceptor()).after(NonTransactionalLockingInterceptor.class)
    .interceptor(new BeforeInterceptor()).before(CallInterceptor.class)
  .build();
