package pl.edu.mimuw.students.mw336071.commons;

import java.util.List;
import java.util.Map;

/**
 * Created by m on 11.03.15.
 */
public class MinhashingOutput {
	public MinhashingOutput() {
	}

	public MinhashingOutput(Map<String, List<Long>> docNameToSignature) {
		this.docNameToSignature = docNameToSignature;
	}

	public Map<String, List<Long>> getDocNameToSignature() {
		return docNameToSignature;
	}

	public void setDocNameToSignature(Map<String, List<Long>> docNameToSignature) {
		this.docNameToSignature = docNameToSignature;
	}

	private Map<String, List<Long>> docNameToSignature;


}
