package at.dobiasch.mapreduce.framework.partition;

public interface IPartitioner
{
	public int getPartition(String key, Object value, int numPartitions);
}
