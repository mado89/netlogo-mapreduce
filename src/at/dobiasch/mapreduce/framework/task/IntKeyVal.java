package at.dobiasch.mapreduce.framework.task;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Helper-Datastructure for intermediate Key-value pairs
 */
public class IntKeyVal
{
	/**
	 * Filename
	 */
	public String fn;
	/**
	 * The output file to write out the values
	 */
	private BufferedWriter out;
	
	/**
	 * Number of values for this key
	 */
	int count;
	
	public IntKeyVal(String fn) throws IOException
	{
		this.fn= fn;
		out= new BufferedWriter(new FileWriter(new File(fn),false));
		count= 0;
	}
	
	public synchronized void writeValue(String value) throws IOException
	{
		if( out == null )
			reopen();
		out.write(value + "\n");
		out.close();
		out= null;
		count++;
	}
	
	public void reopen() throws IOException
	{
		out= new BufferedWriter(new FileWriter(new File(fn),true));
	}
	
	public void close() throws IOException
	{
		if( out != null )
		{
			out.close();
			out= null;
		}
	}
}
