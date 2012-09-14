package at.dobiasch.mapreduce.framework.partition;

public class HashPartitioner implements IPartitioner
{
	/*private int hashCode(String s)
	{
		byte[] bytes = s.getBytes();
		return (bytes == null) ? 31 : Arrays.hashCode(bytes);
	}*/

	@Override
	public int getPartition(String key, Object value, int numPartitions)
	{
		return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
	}
}
