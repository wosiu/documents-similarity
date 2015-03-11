package hashing;

/**
 * Created by m on 10.03.15.
 */
public class LinearHashFunction implements HashFunction {
	private long a, b;

	public LinearHashFunction(long a, long b) {
		this.a = a;
		this.b = b;
	}
	@Override
	public long hash(long number) {
		return a * number + b;
	}
}
