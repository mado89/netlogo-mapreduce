package at.dobiasch.mapreduce.framework.partition;

public class Data
{
	public String key;
	public String checksum;
	public int    numpartitions;
	public String partitionfile;
	public long   lastpartitionend;
	
	public Data()
	{
		key= checksum= partitionfile= "";
		numpartitions= 0;
		lastpartitionend= 0;
	}
	
	public Data(String data)
	{
		String[] h= data.split(",");
		key= h[0];
		checksum= h[1];
		numpartitions= Integer.parseInt(h[2]);
		partitionfile= h[3];
		lastpartitionend= Integer.parseInt(h[4]);
	}
	
	public String toString()
	{
		return "[" + key + "," + checksum + "," + numpartitions + "," + 
			partitionfile + "," + lastpartitionend + "]";
	}
}
