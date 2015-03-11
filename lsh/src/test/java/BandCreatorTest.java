import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Created by m on 11.03.15.
 */
public class BandCreatorTest {
	@Test
	public void bandCreatorTest() throws Exception {
		BandCreator bandCreator = new BandCreator(2);
		List<Long> docSignature = Arrays.asList(new Long[]{1L, 2L, 3L, 4L, 5L});

		List<Band> bands = bandCreator.create(docSignature);
		assertEquals(2, bands.size());

		assertEquals(new Band(0, Arrays.asList(new Long[]{1L, 2L})), bands.get(0));
		assertEquals(new Band(1, Arrays.asList(new Long[]{3L, 4L})), bands.get(1));
	}
}
