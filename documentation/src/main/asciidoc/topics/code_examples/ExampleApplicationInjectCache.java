public class ExampleApplication {
    @Resource(lookup = "java:jboss/datagrid-infinispan/container/infinispan_container")
    CacheContainer container;

    @Resource(lookup = "java:jboss/datagrid-infinispan/container/infinispan_container/cache/namedCache")
    Cache cache;
}
