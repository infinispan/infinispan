@Text(projectable = true, analyzer = "whitespace")
@ProtoField(value = 1)
private String id;

@Text(projectable = true, analyzer = "simple")
@ProtoField(value = 2)
private String description;
