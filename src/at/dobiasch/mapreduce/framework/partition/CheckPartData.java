package at.dobiasch.mapreduce.framework.partition;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class CheckPartData
{
	private Map<String,Data> map;
	// private Map<String,String> checks;
	private int numpartitions;
	
	public CheckPartData()
	{
		map= new HashMap<String,Data>();
		numpartitions= 0;
		// checks= new HashMap<String,String>();
	}
	
	public CheckPartData(String data)
	{
		map= new HashMap<String,Data>();
		fromString(data);
		// checks= new HashMap<String,String>();
	}

	/**
	 * Add a checksum entry
	 * @param key
	 * @param checksum
	 
	public void putChecksum(String key, String checksum) {
		checks.put(key, checksum);
	}

	public Map<String, String> getChecksums() {
		return checks;
	}*/

	/**
	 * Add an entry. Also adds a checksum entry
	 * @param key
	 * @param ret
	 */
	public void put(String key, Data ret) {
		map.put(key, ret);
		numpartitions+= ret.numpartitions;
		// checks.put(key, ret.checksum);
	}

	public Collection<Data> values() {
		return map.values();
	}
	
	public String toString()
	{
		return map.toString();
	}
	
	public int getNumberOfPartitions()
	{
		return numpartitions;
	}
	
	private void fromString(String data)
	{
		int i;
		String[] files;
		String[] file;
		
		data= data.substring(1);
		data= data.substring(0, data.length() - 2);
		
		files= data.split("], ");
		for(i= 0; i < files.length; i++)
		{
			
			file= files[i].split("=\\[");
			put(file[0], new Data(file[1]));
			// key= file[0];
			// dat= file[1].substring(2);
		}
	}
	
	/*@Override
	public boolean equals(Object d)
	{
		//TODO: implement me
		if( d.getClass() != this.getClass()) throw new IllegalArgumentException("Cant compare CheckPartData with " + d.getClass());
		
		return false;
	}*/
	
	/**
	 * Compare two Data-Objects. Return the difference between them
	 * Difference: a - b: check for all files in a if they are in b (and if checksums are equal), all files not in b are the difference 
	 * for all files in this object its checked whether they are in d (and if checksums are equal) (= ret 0)
	 *  for all files in d its checked whether they are in this object (and if checksums are equal) (=ret 1)
	 *  a: {f1=[checksum=1],f2=[checksum=2],f3=[checksum=3]} b:{f1=[checksum=1],f2=[checksum=4]}
	 *   a - b: {f2=[checksum=2],f3=[checksum=3]}
	 *   b - a: {f2=[checksum=4]}
	 * @param d
	 * @return a two dimensional array. [0] this - d, [1]: d - this
	 */
	public CheckPartData[] getDifference(CheckPartData d)
	{
		CheckPartData diff[]= new CheckPartData[2];
		diff[0]= new CheckPartData();
		diff[1]= new CheckPartData();
		
		for(Entry<String,Data> e : this.map.entrySet())
		{
			if( !d.map.containsKey(e.getKey()) )
			{
				diff[0].put(e.getKey(), e.getValue());
			}
			else
			{
				Data dd= d.map.get(e.getKey());
				if( ! e.getValue().checksum.equals( dd.checksum ) )
				{
					diff[0].put(e.getKey(), dd);
				}
			}
		}
		
		for(Entry<String,Data> e : d.map.entrySet())
		{
			if( !this.map.containsKey(e.getKey()) )
			{
				diff[0].put(e.getKey(), e.getValue());
			}
			else
			{
				Data dd= this.map.get(e.getKey());
				if( ! e.getValue().checksum.equals( dd.checksum ) )
				{
					diff[0].put(e.getKey(), dd);
				}
			}
		}
		
		return diff;
	}
}
