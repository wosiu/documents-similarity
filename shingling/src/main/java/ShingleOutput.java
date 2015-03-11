import java.util.List;
import java.util.Map;

/**
 * Created by m on 11.03.15.
 */
public class ShingleOutput {
	public long getShingleSumNumber() {
		return shingleSumNumber;
	}

	public void setShingleSumNumber(long shingleSumNumber) {
		this.shingleSumNumber = shingleSumNumber;
	}

	private long shingleSumNumber;

	public Map<String, List<Long>> getFileNameToShingleIds() {
		return fileNameToShingleIds;
	}

	public void setFileNameToShingleIds(Map<String, List<Long>> fileNameToShingleIds) {
		this.fileNameToShingleIds = fileNameToShingleIds;
	}

	private Map<String, List<Long>> fileNameToShingleIds;
	public ShingleOutput(long shingleSumNumber, Map<String, List<Long>> fileNameToShingleIds) {
		this.shingleSumNumber = shingleSumNumber;
		this.fileNameToShingleIds = fileNameToShingleIds;
	}
	public ShingleOutput() {
	}
}
