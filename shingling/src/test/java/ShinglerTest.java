import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Created by m on 10.03.15.
 */
public class ShinglerTest {
	@Test
	public void createTest() throws Exception {
		Shingler shingler = new Shingler(3);
		String document = "Abc d - ef.";
		List<String> shingle = shingler.create(document);
		String[] valid = {" - ", " d ", " ef", "- e", "Abc", "bc ", "c d", "d -", "ef."};
		assertArrayEquals(valid, shingle.toArray());
	}
}
