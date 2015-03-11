import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Created by m on 11.03.15.
 */
public class PairsCreator extends EvalFunc<DataBag> {

	public List<Tuple> create(List<String> docNames) {
		Collections.sort(docNames);
		List<Tuple> result = new ArrayList<Tuple>();
		for ( int i = 1; i < docNames.size(); i++ ) {
			for ( int j = 0; j < i; j++ ) {
				Tuple pair = TupleFactory.getInstance().newTuple();
				pair.append(docNames.get(j));
				pair.append(docNames.get(i));
				result.add(pair);
			}
		}
		return result;
	}

	public DataBag exec(Tuple input) throws IOException {
		if (input == null || input.size() != 1) {
			return null;
		}
		DataBag bucketDataBag = (DataBag) input.get(0);
		Iterator<Tuple> it = bucketDataBag.iterator();
		List<String> docNames = new ArrayList<String>((int) bucketDataBag.size());

		while(it.hasNext()){
			Tuple docNameTuple = it.next();
			if (docNameTuple == null || docNameTuple.size() != 1) {
				continue;
			}
			String docName = (String) docNameTuple.get(0);
			docNames.add(docName);
		}

		DataBag result = new DefaultDataBag(create(docNames));
		return result;
	}
}
