@Inject
public void injectDependencies(DataContainer container, Configuration configuration) {
   this.container = container;
   this.configuration = configuration;
}

@DefaultFactoryFor
public class DataContainerFactory extends AbstractNamedCacheComponentFactory {
