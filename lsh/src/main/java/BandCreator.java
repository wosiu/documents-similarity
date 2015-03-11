import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.Tuple;

import javax.xml.crypto.Data;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by m on 11.03.15.
 */
public class BandCreator extends EvalFunc<DataBag> {
	private int bandSize;

	public BandCreator(int bandSize){
		this.bandSize = bandSize;
	}

	public BandCreator(String bandSize){
		this.bandSize = Integer.valueOf(bandSize);
	}


	public List<Band> create(List<Long> docSignature) {
		return create(docSignature, null);
	}

	public List<Band> create(List<Long> docSignature, String docname) {
		List<Band> bands = new ArrayList<Band>();
		List<Long>bandSignature = new ArrayList<Long>(bandSize);
		int bandLevel = 0;
		int i = 0;
		for (long s : docSignature) {
			i++;
			bandSignature.add(s);
			if ( i % bandSize == 0 ) {
				bands.add(new Band(bandLevel, bandSignature, docname));
				bandSignature = new ArrayList<Long>(bandSize);
				bandLevel++;
			}

		}
		return bands;
	}

	@Override
	public DataBag exec(Tuple tuple) throws IOException {
		if (tuple == null || tuple.size() != 1) {
			return null;
		}
		DataBag signatureDataBag = (DataBag) tuple.get(0);
		if (signatureDataBag == null ) {
			return null;
		}
		Iterator<Tuple> it = signatureDataBag.iterator();
		List<Long> signature = new ArrayList<Long>((int) signatureDataBag.size());
		while (it.hasNext()) {
			Tuple t = it.next();
			if ( t == null || t.size() != 1) {
				continue;
			}
			Long s = (Long) t.get(0);
			if ( s == null ) {
				continue;
			}
			signature.add(s);
		}

		List <Band> bands = create(signature);
		DataBag result = new DefaultDataBag();
		for (Band band : bands ) {
			result.add(band.toPigStructure());
		}
		return result;
	}
}
