@Entity
public class User implements Serializable {
	@Id
	private String username;
	private String firstName;
	private String lastName;

	...
}
