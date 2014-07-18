/**
 * <h1>Spring Infinispan - An implementation of Spring 3.2's Cache SPI based on JBoss Infinispan.</h1>
 * <p>
 * Spring 3.1 introduces caching capabilities a user may comfortably utilize via a set of custom annotations, thus telling
 * the Spring runtime which objects to cache under which circumstances.</br>
 * Out of the box, Spring ships with <a href="">EHCache</a> as the caching provider to delegate to. It defines, however, a
 * simple SPI vendors may implement for their own caching solution, thus enabling Spring users to swap out the default
 * EHCache for another cache of their choosing. This SPI comprises two interfaces:
 * <ul>
 *   <li>
 *     {@link org.springframework.cache.Cache <code>Cache</code>}, Spring's cache abstraction itself, and
 *   </li>
 *   <li>
 *     {@link org.springframework.cache.CacheManager <code>CacheManager</code>}, a service for creating <code>Cache</code>
 *     instances
 *   </li>
 * </ul>
 * <em>Spring Infinispan</em> implements this SPI for JBoss Infinispan.
 * </p>
 * <p>
 * While <em>Spring Infinispan</em> offers only one implementation of <code>org.springframework.cache.Cache</code>, namely
 * {@link org.infinispan.spring.provider.SpringCache <code>org.infinispan.spring.provider.SpringCache</code>}, there are two implementations
 * of <code>org.springframework.cache.CacheManager</code>:
 * <ol>
 *   <li>
 *     {@link org.infinispan.spring.provider.SpringEmbeddedCacheManager <code>org.infinispan.spring.provider.SpringEmbeddedCacheManager</code>}
 *     and
 *   </li>
 *   <li>
 *     {@link org.infinispan.spring.provider.SpringRemoteCacheManager <code>org.infinispan.spring.provider.SpringRemoteCacheManager</code>}.
 *   </li>
 * </ol>
 * These two implementations cover two distinct use cases:
 * <ol>
 *   <li>
 *     <strong>Embedded</strong>: Embed your Spring-powered application into the same JVM running an Infinispan node, i.e. every
 *     communication between application code and Infinispan is in-process. Infinispan supports this use case via the interface
 *     {@link org.infinispan.manager.EmbeddedCacheManager <code>org.infinispan.manager.EmbeddedCacheManager</code>} and its default
 *     implementation {@link org.infinispan.manager.DefaultCacheManager <code>org.infinispan.manager.DefaultCacheManager</code>}. The
 *     latter backs {@link org.infinispan.spring.provider.SpringEmbeddedCacheManager <code>SpringEmbeddedCacheManager</code>}.
 *   </li>
 *   <li>
 *     <strong>Remote</strong>: Application code accesses Infinispan nodes remotely using Infinispan's own <em>hotrod</em>
 *     protocol. Infinispan supports this use case via {@link org.infinispan.client.hotrod.RemoteCacheManager
 *     <code>org.infinispan.client.hotrod.RemoteCacheManager</code>}. {@link org.infinispan.spring.provider.SpringRemoteCacheManager
 *     <code>SpringRemoteCacheManager</code>} delegates to it.
 *   </li>
 * </ol>
 * </p>
 * <strong>Usage</strong>
 * <p>
 * Using <em>Spring Infinispan</em> as a Spring Cache provider may be divided into two broad areas:
 * <ol>
 *   <li>
 *     Telling the Spring runtime to use <em>Spring Infinispan</em> and therefore Infinispan as its caching provider.
 *   </li>
 *   <li>
 *     Using Spring's caching annotations in you application code.
 *   </li>
 * </ol>
 * </p>
 * <p>
 * <em>Register Spring Infinispan with the Spring runtime</em>
 * <p>
 * Suppose we want to use <em>Spring Infinispan</em> running in embedded mode as our caching provider, and suppose further that 
 * we want to create two named cache instances, &quot;cars&quot; and &quot;planes&quot;. To that end, we put
 * <pre>
 * &lt;beans xmlns="http://www.springframework.org/schema/beans"
 *        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
 *        xmlns:cache="http://www.springframework.org/schema/cache"
 *        xmlns:p="http://www.springframework.org/schema/p"
 *        xsi:schemaLocation="http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans.xsd
 *               http://www.springframework.org/schema/cache http://www.springframework.org/schema/cache/spring-cache.xsd"&gt;
 *     &lt;cache:annotation-driven /&gt;
 *
 *     &lt;bean id="cacheManager" class="org.infinispan.spring.SpringEmbeddedCacheManagerFactoryBean"
 *              p:configuration-file-location="classpath:/org/infinispan/spring/embedded/example/infinispan-sample-config.xml"/&gt;
 *
 * &lt;/beans&gt;
 * </pre>
 * in our Spring application context. It is important to note that <code>classpath:/org/infinispan/spring/embedded/example/infinispan-sample-config.xml</code>
 * points to a configuration file in the standard Infinispan configuration format that includes sections for two named caches
 * &quot;cars&quot; and &quot;planes&quot;. If those sections are missing the above application context will still work, yet the
 * two caches &quot;cars&quot; and &quot;planes&quot; will be configured using the default settings defined in
 * <code>classpath:/org/infinispan/spring/embedded/example/infinispan-sample-config.xml</code>.<br/>
 * To further simplify our setup we may omit the reference to an Infinispan configuration file in which case the underlying
 * {@link org.infinispan.manager.EmbeddedCacheManager <code>org.infinispan.manager.EmbeddedCacheManager</code>} will use Infinispan's
 * default settings. 
 * </p>
 * <p>
 * For more advanced ways to configure the underlying Infinispan <code>EmbeddedCacheManager</code> see 
 * {@link org.infinispan.spring.provider.SpringEmbeddedCacheManagerFactoryBean <code>org.infinispan.spring.provider.SpringEmbeddedCacheManagerFactoryBean</code>}.
 * </p>
 * <p>
 * If running Infinispan in remote mode the above configuration changes to
 * <pre>
 * &lt;beans xmlns="http://www.springframework.org/schema/beans"
 *        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
 *        xmlns:cache="http://www.springframework.org/schema/cache"
 *        xmlns:p="http://www.springframework.org/schema/p"
 *        xsi:schemaLocation="http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans.xsd
 *               http://www.springframework.org/schema/cache http://www.springframework.org/schema/cache/spring-cache.xsd"&gt;
 *     &lt;cache:annotation-driven /&gt;
 *
 *     &lt;bean id="cacheManager" class="org.infinispan.spring.SpringEmbeddedCacheManagerFactoryBean"
 *              p:configuration-properties-file-location="classpath:/org/infinispan/spring/remote/example/hotrod-client-sample.properties"/&gt;
 *
 * &lt;/beans&gt;
 * </pre>
 * </p>
 * <p>
 * For more advanced ways to configure the underlying Infinispan <code>RemoteCacheManager</code> see 
 * {@link org.infinispan.spring.provider.SpringRemoteCacheManagerFactoryBean <code>org.infinispan.spring.provider.SpringRemoteCacheManagerFactoryBean</code>}.
 * </p>
 * <em>Using Spring's caching annotations in application code</em>
 * <p>
 * A detailed discussion about how to use Spring's caching annotations {@link org.springframework.cache.annotation.Cacheable <code>@Cacheable</code>}
 * and {@link org.springframework.cache.annotation.CacheEvict <code>@CacheEvict</code>} is beyond this documentation's scope. A simple example may
 * serve as a starting point:
 * <pre>
 * import org.springframework.cache.annotation.CacheEvict;
 * import org.springframework.cache.annotation.Cacheable;
 * import org.springframework.stereotype.Repository;
 *
 * &#064;Repository
 * public class CarRepository {
 *
 *   &#064;Cacheable("cars")
 *   public Car getCar(Long carId){
 *       ...
 *   }
 *
 *   &#064;CacheEvict(value="cars", key="car.id")
 *   public void saveCar(Car car){
 *       ...
 *   }
 * }
 * </pre>
 * In both <code>&#064;Cache("cars")</code> and <code>&#064;CacheEvict(value="cars", key="car.id")</code> &quot;cars&quot; refers to the name of the cache to either
 * store the returned <code>Car</code> instance in or to evict the saved/updated <code>Car</code> instance from. For a more detailed explanation of
 * how to use <code>&#064;Cacheable</code> and <code>&#064;CacheEvict</code> see the relevant reference documentation 
 * <a href="http://docs.spring.io/spring/docs/3.2.9.RELEASE/spring-framework-reference/html/cache.html">chapter</a>.
 * </p>
 */
package org.infinispan.spring.provider;

