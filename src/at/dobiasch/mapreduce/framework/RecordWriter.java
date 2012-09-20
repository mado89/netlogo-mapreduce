package at.dobiasch.mapreduce.framework;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;

public class RecordWriter
{
	private long ID;
	private boolean session;
	private long sessStart;
	private RandomAccessFile out;
	private final byte[] keyValueSeparator;
	
	/*
	 * Code and idea taken from apache hadoop
	 */
	private static final String utf8 = "UTF-8";
	private static final byte[] newline;
	static {
	      try {
	        newline = "\n".getBytes(utf8);
	      } catch (UnsupportedEncodingException uee) {
	        throw new IllegalArgumentException("can't find " + utf8 + " encoding");
	      }
	    }
	
	public RecordWriter(String filename, String keyValueSeparator, long pos) throws IOException
	{
		this.out = new RandomAccessFile(filename, "rw");
		this.out.seek(pos);
		this.session= false;
		this.sessStart= 0;
		
		try {
			this.keyValueSeparator = keyValueSeparator.getBytes(utf8);
		} catch (UnsupportedEncodingException uee) {
			throw new IllegalArgumentException("can't find " + utf8
					+ " encoding");
		}
	}
	
	public RecordWriter(String filename, String keyValueSeparator) throws IOException
	{
		this.out = new RandomAccessFile(filename, "rw");
		this.out.seek(0);
		this.session= false;
		this.sessStart= 0;
		
		try {
			this.keyValueSeparator = keyValueSeparator.getBytes(utf8);
		} catch (UnsupportedEncodingException uee) {
			throw new IllegalArgumentException("can't find " + utf8
					+ " encoding");
		}
	}
	
	public void startSession(long ID)
	{
		this.ID= ID;
		this.session= true;
	}
	
	public void endSession() throws IOException
	{
		this.session= false;
		
		this.sessStart= this.out.getFilePointer();
	}
	
	/**
	 * Unvalidate the current Session and remove any content from it
	 * Also ends the session
	 * @throws IOException 
	 */
	public void removeSession() throws IOException
	{
		this.out.seek(this.sessStart);
		this.out.setLength(this.sessStart);
		
	}
	
	public void write(String key, String value) throws IOException
	{
		this.out.write(key.getBytes(utf8));
		this.out.write(keyValueSeparator);
		this.out.write(value.getBytes(utf8));
		this.out.write(newline);
	}
	
	public void close() throws IOException
	{
		this.out.close();
	}
}
