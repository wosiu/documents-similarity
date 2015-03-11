import org.apache.pig.EvalFunc;
import org.apache.pig.data.*;

import java.io.IOException;
import java.util.*;

/**
 * Created by m on 10.03.15.
 */
public class Shingler extends EvalFunc<DataBag> {
	private int gramSize;

	public Shingler(int gramSize) {
		this.gramSize = gramSize;
	}

	public Shingler(String gramSize) {
		this.gramSize = Integer.valueOf(gramSize);
	}

	public List<String> create(String document) {
		Set<String> shingle = new HashSet<String>();
		if (document.length() < gramSize) {
			return null;
		}
		// TODO we can use suffix tree to avoid substring eash k-subword and using hashset
		for (int i = 0; i + gramSize - 1 < document.length(); i++) {
			String shingl = document.substring(i, i + gramSize);
			shingle.add(shingl);
		}

		List<String> sortedShingle = new ArrayList<String>(shingle);
		Collections.sort(sortedShingle);
		return sortedShingle;
	}

	@Override
	public DataBag exec(Tuple tuple) throws IOException {
		if (tuple == null || tuple.size() == 0) {
			return null;
		}
		String document = (String) tuple.get(0);
		List<String> shingle = create(document);
		DataBag result = new DefaultDataBag();
		for (String sh : shingle) {
			Tuple t = TupleFactory.getInstance().newTuple();
			t.append(sh);
			result.add(t);
		}
		return result;
	}
}
