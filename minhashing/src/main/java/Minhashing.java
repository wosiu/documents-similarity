import hashing.HashFunction;
import hashing.HashFunctionGenerator;
import hashing.SimpleHashFunctionGenerator;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by m on 10.03.15.
 */
public class Minhashing extends EvalFunc<DataBag> {

	private List<HashFunction> hashFunctions;

	private void init(int signaturesNumber) {
		HashFunctionGenerator hashFunctionGenerator = new SimpleHashFunctionGenerator();
		hashFunctions = hashFunctionGenerator.generateHashFunctionsList(signaturesNumber);
	}
	/**
	 * @param signaturesNumber - number of signatures for each document on output.
	 *                         Equal number of hash functions.
	 */
	public Minhashing(int signaturesNumber) {
		init(signaturesNumber);
	}

	public Minhashing(String signaturesNumber) {
		init(Integer.valueOf(signaturesNumber));
	}
	/**
	 * Creates ducument signature respecting hashing functions and all shingles order.
	 * @param shingleIds - indexes of document shingles in shingles sum list
	 * @param mod - max index number in set of shingle sum
	 * @return
	 */
	public List<Integer> createSignature(List<Integer> shingleIds, int mod) {
		List<Integer> signature = new ArrayList<Integer>(hashFunctions.size());

		for (HashFunction hashFunction : hashFunctions) {
			int minId = Integer.MAX_VALUE;
			for (int id : shingleIds) {
				int hashedId = hashFunction.hash(id) % mod + 1;
				minId = Math.min(minId, hashedId);
			}
			signature.add(minId);
		}
		return signature;
	}

	@Override
	public DataBag exec(Tuple tuple) throws IOException {
		System.out.println("WSZEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEDL");
		if (tuple == null || tuple.size() != 2) {
			return null;
		}
		System.out.println("TUUUUUUUUUUUUUUUTAAAAAAAAAAAAJJJJJJJJJJJJJJJJJJ");
		System.out.println(tuple);
		DataBag shingleIdsDataBag = (DataBag) tuple.get(0);
		Long modL = (Long) tuple.get(1);
		if (shingleIdsDataBag == null || modL == null) {
			return null;
		}
		long mod2 = modL;
		int mod = (int) mod2;
		System.out.println(mod);

		Iterator<Tuple> it = shingleIdsDataBag.iterator();
		List<Integer> shingleIds = new ArrayList<Integer>((int) shingleIdsDataBag.size());

		while (it.hasNext()) {
			Tuple t = it.next();
			if (t == null || t.size() != 1) {
				continue;
			}
			int id = (Integer) t.get(0);
			shingleIds.add(id);
		}

		List<Integer> signature = createSignature(shingleIds, mod);

		DataBag result = new DefaultDataBag();
		for (int element : signature) {
			Tuple t = TupleFactory.getInstance().newTuple();
			t.append(element);
			result.add(t);
		}
		return result;
	}
}
