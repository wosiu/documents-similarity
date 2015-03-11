import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Created by m on 11.03.15.
 */
public class BucketsCreatorTest {
	@Test
	public void bandCreatorTest() throws Exception {
		BucketsCreator bucketsCreator = new BucketsCreator();

		List<Band> bands = new ArrayList<Band>();
		bands.add(new Band("a", Arrays.asList(new Long[]{1L, 2L})));
		bands.add(new Band("b", Arrays.asList(new Long[]{2L, 3L})));
		bands.add(new Band("c", Arrays.asList(new Long[]{1L, 2L})));

		Collection<List<String>> buckets = bucketsCreator.create(bands);

		assertEquals(2, buckets.size());
		List<String> bucket1 = Arrays.asList(new String[]{"a", "c"});
		List<String> bucket2 = Arrays.asList(new String[]{"b"});

		assert(buckets.contains(bucket1));
		assert(buckets.contains(bucket2));
	}
}
