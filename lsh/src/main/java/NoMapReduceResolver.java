import org.apache.log4j.Logger;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.codehaus.jackson.map.ObjectMapper;
import pl.edu.mimuw.students.mw336071.commons.MinhashingOutput;

import java.io.FileInputStream;
import java.util.*;

/**
 * Created by m on 11.03.15.
 */
public class NoMapReduceResolver {
	private static final Logger logger = Logger.getLogger(NoMapReduceResolver.class);

	public static void main(String[] args) throws Exception {
		int bandSize = 5;
		if (args.length == 1) {
			bandSize = Integer.valueOf(args[0]);
		} else {
			logger.warn("Invalid arguments number. Set default.");
		}

		String minhashResultFilePath = "minhash-output.json";
		String lshResultFilePath = "shingle-output.out";

		FileInputStream fis = new FileInputStream(minhashResultFilePath);
		MinhashingOutput input = new ObjectMapper().readValue(fis, MinhashingOutput.class);

		Map<String, List<Long>> docNameToSignature = input.getDocNameToSignature();
		int docNumber = docNameToSignature.size();

		// CREATE BANDS FOR EACH DOCUMENT
		List<List<Band>> docToBands =
				new ArrayList<List<Band>>(docNumber);
		BandCreator bandCreator = new BandCreator(bandSize);

		for (Map.Entry<String, List<Long>> entry : docNameToSignature.entrySet()) {
			String docName = entry.getKey();
			List<Long> signature = entry.getValue();
			List<Band> bands = bandCreator.create(signature, docName);
			docToBands.add(bands);
		}

		// CREATE BUCKETS FOR EACH BAND LEVEL
		long bandLevels = docToBands.get(0).size();
		assert(bandLevels > 0);
		BucketsCreator bucketsCreator = new BucketsCreator();
		PairsCreator pairsCreator = new PairsCreator();
		Set<Tuple> docPairs = new HashSet<Tuple>();

		for (int i = 0; i < bandLevels; i++) {
			List<Band> levelBands = new ArrayList<Band>(docNumber);
			for( List<Band> docBands : docToBands ) {
				levelBands.add(docBands.get(i));
			}
			assert(!levelBands.isEmpty());
			// create buckets
			List<List<String>> docBuckets = bucketsCreator.createDocBuckets(levelBands);
			assert(!docBuckets.isEmpty());
			// create documents pair for each bucket
			for ( List<String> docBucket : docBuckets ) {
				List<Tuple> pairs = pairsCreator.create(docBucket);
				docPairs.addAll(pairs);
			}
		}
		System.out.println(docPairs);
	}
}
