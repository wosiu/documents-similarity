import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.util.List;

/**
 * Created by m on 11.03.15.
 */
public class Band {
	public Band(int bandLevel, List<Long> bandSignature, String docName) {
		this.bandLevel = bandLevel;
		this.signature = bandSignature;
		this.docName = docName;
	}

	public Band(int bandLevel, List<Long> bandSignature) {
		this.bandLevel = bandLevel;
		this.signature = bandSignature;
	}

	public Band(String docName, List<Long> bandSignature) {
		this.signature = bandSignature;
		this.docName = docName;
	}


	public List<Long> getSignature() {
		return signature;
	}

	public void setSignature(List<Long> signature) {
		this.signature = signature;
	}

	private List<Long> signature;

	public int getBandLevel() {
		return bandLevel;
	}

	public void setBandLevel(int bandLevel) {
		this.bandLevel = bandLevel;
	}

	private int bandLevel;

	public String getDocName() {
		return docName;
	}

	public void setDocName(String docName) {
		this.docName = docName;
	}

	private String docName;

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof Band)) return false;

		Band band = (Band) o;

		if (bandLevel != band.bandLevel) return false;
		if (!signature.equals(band.signature)) return false;

		return true;
	}

	@Override
	public int hashCode() {
		return signature.hashCode();
	}

	public Tuple toPigStructure() {
		Tuple result = TupleFactory.getInstance().newTuple();
		DataBag signatureDataBag = new DefaultDataBag();
		for ( long s : signature ) {
			Tuple t = TupleFactory.getInstance().newTuple();
			t.append(s);
			signatureDataBag.add(t);
		}
		result.append(signatureDataBag);
		result.append(bandLevel);
		return result;
	}

	@Override
	public String toString() {
		return "Band{" +
				"signature=" + signature +
				'}';
	}
}
