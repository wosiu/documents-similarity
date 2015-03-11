package hashing;

/**
 * Created by m on 10.03.15.
 */
public class LinearHashFunction implements HashFunction {
	private int a, b;

	public LinearHashFunction(int a, int b) {
		this.a = a;
		this.b = b;
	}
	@Override
	public int hash(int number) {
		return a * number + b;
	}
}
