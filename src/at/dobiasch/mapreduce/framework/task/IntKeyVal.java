package at.dobiasch.mapreduce.framework.task;

import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Helper-Datastructure for intermediate Key-value pairs
 */
public class IntKeyVal
{
	@Override
	public String toString() {
		return "IntKeyVal [fn=" + fn + ", key=" + key + ", out=" + out
				+ ", count=" + count + ", fsize=" + fsize + "]";
	}

	/**
	 * Filename
	 */
	public String fn;
	/**
	 * Key
	 */
	public String key;
	/**
	 * The output file to write out the values
	 */
	private FileOutputStream out;
	
	/**
	 * Number of values for this key
	 */
	private int count;
	
	/**
	 * Number of Bytes in the file
	 */
	private long fsize;
	
	public IntKeyVal(String key, String fn) throws IOException
	{
		this.fn= fn;
		this.key= key;
		out= new FileOutputStream(fn,false);
		count= 0;
		fsize= 0;
	}
	
	/*public IntKeyVal(String fn, int count, long size)
	{
		this.fn= fn;
		this.count= count;
		this.fsize= size;
	}*/
	
	public void writeValue(String value) throws IOException
	{
		if( out == null )
			reopen();
		byte[] b= (value + "\n").getBytes();
		out.write(b);
		fsize+= b.length;
		out.close();
		out= null;
		count++;
	}
	
	public void reopen() throws IOException
	{
		out= new FileOutputStream(fn,true);
	}
	
	public void close() throws IOException
	{
		if( out != null )
		{
			out.close();
			out= null;
		}
	}
	
	public int getCount()
	{
		return count;
	}

	public long getFileSize()
	{
		return fsize;
	}
}
