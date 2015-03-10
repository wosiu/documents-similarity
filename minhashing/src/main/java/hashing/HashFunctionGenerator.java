package hashing;

import java.util.List;

/**
 * Created by m on 10.03.15.
 */
public interface HashFunctionGenerator {
	public List<HashFunction> generateHashFunctionsList(int numberOfFunctions, int maxHashValue);
}
