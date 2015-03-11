import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by m on 10.03.15.
 */
public class DataBagStringConcate extends EvalFunc<Tuple> {

	@Override
	public Tuple exec(Tuple tuple) throws IOException {
		if ( tuple == null || tuple.size() == 0 ) {
			return null;
		}
		DataBag paragraphsDataBag = (DataBag) tuple.get(0);
		Iterator<Tuple> it = paragraphsDataBag.iterator();
		StringBuilder stringBuilder = new StringBuilder();

		while (it.hasNext()) {
			Tuple t = it.next(); // 0 - docname, 1 - paragraph
			if ( t == null || t.size() != 1) {
				continue;
			}
			String paragraph = (String) t.get(0);
			if (paragraph == null || paragraph.isEmpty()) {
				continue;
			}
			stringBuilder.append(paragraph);
			stringBuilder.append("\n");
		}

		String document = stringBuilder.toString();
		Tuple result = TupleFactory.getInstance().newTuple();
		result.append(document);
		return result;
	}
}
