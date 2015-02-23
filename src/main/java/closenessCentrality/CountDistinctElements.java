package closenessCentrality;

import java.util.Arrays;

import com.google.common.base.Preconditions;

/* Flajolet-Martin Sketch: Count distinct number of values
 *in a sequence in one pass with minimum memory 
 *The main usage is getCount()
 *
 *Iteratively updates the Flajolet-Martin (FM) bitstrings for every node.
 *let b(r - 1; v) be node v’s bitstring encoding the set of nodes within distance
 *r - 1. Then the next-step bitstring b(r; v) is computed by BITWISE-OR the current bitstring b(r - 1; v) of v and
 *the current bitstrings of the neighbors of v.
 */
public class CountDistinctElements {

	public final static int NUM_BUCKETS = 32;
	private final static double MAGIC_CONSTANT = 0.77351;
	private int[] buckets;

	/*
	 * Create a zero-bucket FM-Sketch. This is needed because Giraph requires a
	 * no-argument constructor.
	 */
	public CountDistinctElements() {
		this.buckets = new int[NUM_BUCKETS];
	}

	/*
	 * Create a copy of the FM-Sketch by copying the internal integer array.
	 */
	public CountDistinctElements copy() {
		CountDistinctElements result = new CountDistinctElements();
		result.buckets = Arrays.copyOf(this.buckets, this.buckets.length);
		return result;
	}

	/*
	 * Count the passed in node id.
	 */
	public void addNode(int n) {
		for (int i = 0; i < buckets.length; i++) {
			int hash = hash(n, i);
			buckets[i] |= (1 << Integer.numberOfTrailingZeros(hash));
		}
	}

	/*
	 * Return the estimate for the number of unique ids.
	 */
	public int getCount() {
		int S = 0;
		int R = 0;
		int bucket = 0;
		int[] sorted = new int[buckets.length];
		for (int i = 0; i < buckets.length; ++i) {
			R = 0;
			bucket = buckets[i];
			while ((bucket & 1) == 1 && R < Integer.SIZE) {
				++R;
				bucket >>= 1;
			}
			sorted[i] = R;
		}
		Arrays.sort(sorted);
		int start = (int) (0.25 * buckets.length);
		int end = (int) (0.75 * buckets.length);
		int size = end - start;
		for (int i = start; i < end; i++) {
			S += sorted[i];
		}

		int count = (int) (Math.pow(2.0, (double) S / (double) size) / MAGIC_CONSTANT);
		return count;
	}

	/*
	 * Merge this FM-Sketch with the other one.
	 */
	public void merge(CountDistinctElements other) {
		Preconditions.checkArgument(other instanceof CountDistinctElements,
				"Other is not a FMCounterWritable.");
		CountDistinctElements otherB = (CountDistinctElements) other;
		Preconditions.checkState(this.buckets.length == otherB.buckets.length,
				"Number of buckets does not match.");
		for (int i = 0; i < buckets.length; ++i) {
			buckets[i] |= otherB.buckets[i];
		}
	}

	/*
	 * hash function use for the FM sketch
	 * 
	 * @param code value (node id) to hash
	 * 
	 * @param level to create different hashes for the same value
	 * 
	 * @return hash value (31 significant bits)
	 */
	private int hash(int code, int level) {
		final int rotation = level * 11;
		code = (code << rotation) | (code >>> -rotation);
		code = (code + 0x7ed55d16) + (code << 12);
		code = (code ^ 0xc761c23c) ^ (code >>> 19);
		code = (code + 0x165667b1) + (code << 5);
		code = (code + 0xd3a2646c) ^ (code << 9);
		code = (code + 0xfd7046c5) + (code << 3);
		code = (code ^ 0xb55a4f09) ^ (code >>> 16);
		return code >= 0 ? code : -(code + 1);
	}

	/*
	 * Return the number of buckets.
	 */
	public int getNumBuckets() {
		return buckets.length;
	}

	/*
	 * public void write(DataOutput out) throws IOException {
	 * out.writeInt(buckets.length); for (int b : buckets) { out.writeInt(b); }
	 * } public void read(DataInput in) throws IOException { int numBuckets =
	 * in.readInt(); buckets = new int[numBuckets]; for (int i = 0; i <
	 * buckets.length; ++i) { buckets[i] = in.readInt(); }
	 * 
	 * }
	 */

	/*
	 * public int compareTo(Object arg0) { if(this.hashCode()==arg0.hashCode())
	 * return 1; else return 0; }
	 */

}
