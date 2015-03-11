import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.io.IOException;
import java.util.*;

/**
 * Created by m on 11.03.15.
 */
public class BucketsCreator extends EvalFunc<DataBag> {

	/**
	 * @param bands
	 * @return mapping band signature to buckets
	 */
	public Map<List<Long>, List<String>> createMapping(List<Band> bands) {
		// mapping signatures (List<Long>) to documents name with same signature (List<String>)
		Map<List<Long>, List<String>> mapping = new HashMap<List<Long>, List<String>>();
		for (Band band : bands) {
			List<Long> bandSignature = band.getSignature();
			String docName = band.getDocName();
			List<String> documents = mapping.get(bandSignature);
			if (documents == null) {
				List<String> newDocuments = new ArrayList<String>();
				newDocuments.add(docName);
				mapping.put(bandSignature, newDocuments);
			} else {
				documents.add(docName);
			}
		}
		return mapping;
	}

	/**
	 * @param bands
	 * @return list of documents name with same band signature.
	 */
	Collection<List<String>> create(List<Band> bands) {
		Map<List<Long>, List<String>> mapping = createMapping(bands);
		Collection<List<String>> buckets = mapping.values();
		return buckets;
	}

	@Override
	public DataBag exec(Tuple tuple) throws IOException {
		if (tuple == null || tuple.size() != 1){
			return null;
		}
		DataBag bandsDataBag = (DataBag) tuple.get(0);
		if (bandsDataBag == null || bandsDataBag.size() == 0) {
			return null;
		}
		Iterator<Tuple> it = bandsDataBag.iterator();
		List<Band> bands = new ArrayList<Band>((int) bandsDataBag.size());

		while(it.hasNext()) {
			Tuple bandTuple = it.next();
			if (bandTuple==null || bandTuple.size() < 2) {
				continue;
			}
			String docName = (String) bandTuple.get(0);
			DataBag bandSignatureDataBag = (DataBag) bandTuple.get(1);
			if (docName == null || bandSignatureDataBag == null || docName.isEmpty() ||
					bandSignatureDataBag.size() == 0) {
				continue;
			}
			List<Long> bandSignature = new ArrayList<Long>((int) bandSignatureDataBag.size());
			Iterator<Tuple> bandSignatureIt = bandsDataBag.iterator();
			while (bandSignatureIt.hasNext()) {
				Tuple s = bandSignatureIt.next();
				if (s == null || s.size() != 1) {
					continue;
				}
				bandSignature.add((Long) s.get(0));
			}
			bands.add(new Band(docName, bandSignature));
		}
		System.out.println("TUUUUUUUUUUUUUUUTAAAAAAAAAAAAAAAAAAAAAAAAJJJJJJJJJJJJJ");
		System.out.println(bands.size());
		Collection<List<String>> buckets = create(bands);
		System.out.println(buckets.size());

		Iterator<List<String>> bucketIt = buckets.iterator();
		DataBag result = new DefaultDataBag();
		while (bucketIt.hasNext()) {
			List<String> bucket = bucketIt.next();
			DataBag bucketDataBag = new DefaultDataBag();
			for(String docName : bucket) {
				Tuple docTuple = TupleFactory.getInstance().newTuple();
				docTuple.append(docName);
				bucketDataBag.add(docTuple);
			}
			Tuple bucketTuple = TupleFactory.getInstance().newTuple();
			bucketTuple.append(bucketDataBag);
			result.add(bucketTuple);
		}
		return result;
	}
}
