JChannel jchannel = new JChannel();
// Configure the jchannel as needed.
ConfigurationBuilderHolder holder = new ConfigurationBuilderHolder();
holder.getGlobalConfigurationBuilder()
    .transport().transport(new JGroupsTransport(jchannel));
// Further configuration as needed.
DefaultCacheManager cacheManager = new DefaultCacheManager(holder);
