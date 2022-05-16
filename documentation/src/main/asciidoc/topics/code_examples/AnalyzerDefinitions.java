@Field(store = Store.YES, analyze = Analyze.YES, analyzer = @Analyzer(definition = "keyword"))
@ProtoField(1)
final String id;

@Field(store = Store.YES, analyze = Analyze.YES, analyzer = @Analyzer(definition = "simple"))
@ProtoField(2)
final String description;
