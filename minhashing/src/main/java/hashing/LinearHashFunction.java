package hashing;

/**
 * Created by m on 10.03.15.
 */
public class LinearHashFunction implements HashFunction {
	private int a, b, mod;

	public LinearHashFunction(int a, int b, int mod) {
		this.a = a;
		this.b = b;
		this.mod = mod;
	}
	@Override
	public int hash(int number) {
		return 1 + (a * number + b) % mod;
	}
}
