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
	public List<Long> createSignature(List<Long> shingleIds, long mod) {
		List<Long> signature = new ArrayList<Long>(hashFunctions.size());

		for (HashFunction hashFunction : hashFunctions) {
			long minId = Long.MAX_VALUE;
			for (long id : shingleIds) {
				long hashedId = hashFunction.hash(id) % mod + 1;
				minId = Math.min(minId, hashedId);
			}
			signature.add(minId);
		}
		return signature;
	}

	@Override
	public DataBag exec(Tuple tuple) throws IOException {
		if (tuple == null || tuple.size() != 2) {
			return null;
		}
		DataBag shingleIdsDataBag = (DataBag) tuple.get(0);
		Long mod = (Long) tuple.get(1);
		if (shingleIdsDataBag == null || mod == null) {
			return null;
		}

		Iterator<Tuple> it = shingleIdsDataBag.iterator();
		List<Long> shingleIds = new ArrayList<Long>((int) shingleIdsDataBag.size());

		while (it.hasNext()) {
			Tuple t = it.next();
			if (t == null || t.size() != 1) {
				continue;
			}
			Long id = (Long) t.get(0);
			if ( id == null ) {
				continue;
			}
			shingleIds.add(id);
		}

		List<Long> signature = createSignature(shingleIds, mod);

		DataBag result = new DefaultDataBag();
		for (long element : signature) {
			Tuple t = TupleFactory.getInstance().newTuple();
			t.append(element);
			result.add(t);
		}
		return result;
	}
}
