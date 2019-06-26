@Entity
public class Vehicle implements Serializable {
	@EmbeddedId
	private VehicleId id;
	private String color;	...
}

@Embeddable
public class VehicleId implements Serializable
{
	private String state;
	private String licensePlate;
	...
}
