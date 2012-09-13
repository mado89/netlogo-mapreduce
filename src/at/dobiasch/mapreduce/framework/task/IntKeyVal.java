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
	private int count;
	
	public IntKeyVal(String fn) throws IOException
	{
		this.fn= fn;
		this.count= 0;
		out= new BufferedWriter(new FileWriter(new File(fn)));
	}
	
	public void writeValue(String value) throws IOException
	{
		out.write(value + "\n");
		count++;
	}
	
	public void close() throws IOException
	{
		out.close();
	}
}
