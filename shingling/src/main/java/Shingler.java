import java.util.*;

/**
 * Created by m on 10.03.15.
 */
public class Shingler {
	private int gramSize;

	public Shingler(int gramSize){
		this.gramSize = gramSize;
	}

	public List<String> create(String document) {
		Set<String> shingle = new HashSet<String>();
		if (document.length() < gramSize) {
			return null;
		}
		// TODO we can use suffix tree to avoid substring eash k-subword and using hashset
		for ( int i=0; i + gramSize - 1 < document.length(); i++ ) {
			String shingl = document.substring(i, i + gramSize);
			shingle.add(shingl);
		}

		List<String> sortedShingle = new ArrayList<String>(shingle);
		Collections.sort(sortedShingle);
		return sortedShingle;
	}
}
