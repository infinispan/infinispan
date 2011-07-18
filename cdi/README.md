Infinispan CDI support
======================

This module contains integration with CDI. Configuration and injection of the Inifispan's Cache API is provided, as well
as bridging Cache listeners to the CDI event system. This module also provide a partial support of JSR-107 cache annotations,
for further details see [Chapter 8](https://docs.google.com/document/d/1YZ-lrH6nW871Vd9Z34Og_EqbX_kxxJi55UrSn4yL2Ak/edit?hl=en&pli=1#heading=h.jdfazu3s6oly)
of this specification.

Quick start guide
-----------------

### Inject a Cache

By default you can inject the default Infinispan cache. Let's look at the following example:

    public class GreetingService {

        @Inject
        public Cache<String, String> cache;

        public String greet(String user) {
            String cachedValue = cache.get(user);
            if (cachedValue == null) {
                cachedValue = "Hello " + user;
                cache.put(user, cachedValue);
            }
            return cachedValue;
        }
    }

If you want to use a specific cache you just have to provide your own cache configuration and a cache qualifier. For
example if you want to use a custom cache for the `GreetingService` you have to write your own qualifier (for example
`@GreetingCache`) and define it specific configuration, something like this:

    public class Config {

        @Infinispan("greeting-cache") // this is the cache name
        @GreetingCache // this is the cache qualifier
        @Produces
        public Configuration greetingCacheConfiguration() {
            Configuration configuration = new Configuration();
               configuration.fluent()
                  .eviction()
                  .strategy(FIFO)
                  .maxEntries(10);

            return configuration;
        }

        // the same example but without providing a custom configuration (Default cache configuration will be used).

        @Infinispan("greeting-cache")
        @GreetingCache
        @Produces
        public Configuration greetingCacheConfiguration;
    }

To use this cache in the `GreetingService` you just have to add the qualifier `@GeetingService` on your cache injection.
Very simple isn't it? ;)

### Override the default cache configuration and default manager

It's possible to change the default cache configuration used by the default cache manager. To do this you have to
extend the `DefaultCacheConfigurationProducer` and specialize the producer method. Look at the following example:

    @Specializes
    public static class SmallDefaultCacheConfiguration extends DefaultCacheConfigurationProducer {
      @Override
      public Configuration getDefaultCacheConfiguration() {
         Configuration defaultConfiguration = super.getDefaultCacheConfiguration();
         defaultConfiguration.fluent()
               .eviction()
               .strategy(FIFO)
               .maxEntries(10);

         return defaultConfiguration;
      }

With the same mechanism you can change the default cache manager used by your application. The only one difference is
that you have to specialize the `DefaultCacheManagerProducer` bean. Look at the following example:

    @Specializes
    public class ExternalCacheContainerManager extends DefaultCacheManagerProducer {
       @Override
       public EmbeddedCacheManager getDefaultCacheManager(@Default Configuration defaultConfiguration) {
          EmbeddedCacheManager externalCacheContainerManager = super.getDefaultCacheManager(defaultConfiguration);

          // define large configuration
          Configuration largeConfiguration = new Configuration();
          largeConfiguration.fluent()
            .eviction()
            .strategy(FIFO)
            .maxEntries(100);

          externalCacheContainerManager.defineConfiguration("large", largeConfiguration);

          return externalCacheContainerManager;
       }
    }

### Use JSR-107 cache annotations

The first step is to declare the JSR-107 interceptors in your `beans.xml` file like this:

    <beans xmlns="http://java.sun.com/xml/ns/javaee"
       xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
       xsi:schemaLocation="http://java.sun.com/xml/ns/javaee http://java.sun.com/xml/ns/javaee/beans_1_0.xsd">
        <interceptors>
          <class>org.infinispan.cdi.interceptor.CacheResultInterceptor</class>
          <class>org.infinispan.cdi.interceptor.CacheRemoveEntryInterceptor</class>
          <class>org.infinispan.cdi.interceptor.CacheRemoveAllInterceptor</class>
       </interceptors>
    </beans>

By doing this you'll be able to use the `@CacheResult, @CacheRemoveEntry and @CacheRemoveAll` annotations. For example
you can simplify the above `GreetingService` by simply adding the `@CacheResult` annotation. Look at this:

    public class GreetingService {

        @CacheResult
        public String greet(String user) {
            return "Hello" + user;
        }
    }

If you want to use a specific cache, no problem just fill the `cacheName` attribute of the annotation like this
`@CacheResult(cacheName = "greeting-cache")`.
