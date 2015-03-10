import hashing.HashFunction;
import hashing.HashFunctionGenerator;
import hashing.SimpleHashFunctionGenerator;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by m on 10.03.15.
 */
public class Resolver {

	private final List<HashFunction> hashFunctions;
	private final List<String> sortedShinglesSum;
	/**
	 *
	 * @param sortedShinglesSum - lexicographically sorted set of all possible shingles
	 *                          (sum of shingles from all documents)
	 * @param signaturesNumber - number of signatures for each document on output.
	 *                         Equal number of hash functions.
	 */
	public Resolver(List<String> sortedShinglesSum, int signaturesNumber) {
		this.sortedShinglesSum = sortedShinglesSum;
		HashFunctionGenerator hashFunctionGenerator = new SimpleHashFunctionGenerator();
		hashFunctions = hashFunctionGenerator.generateHashFunctionsList(signaturesNumber,
				sortedShinglesSum.size());
	}

	/**
	 * Creates ducument signature respecting hashing functions and all shingles order.
	 * @param shingleIds - indexes of document shingles in shingles sum list
	 * @return
	 */
	public List<Integer> createSignature(List<Integer> shingleIds) {
		List<Integer> signature = new ArrayList<Integer>(hashFunctions.size());

		for (HashFunction hashFunction : hashFunctions) {
			int min_id = Integer.MAX_VALUE;
			for (int id : shingleIds) {
				min_id = Math.min(min_id, hashFunction.hash(id));
			}
			signature.add(min_id);
		}
		return signature;
	}

}
