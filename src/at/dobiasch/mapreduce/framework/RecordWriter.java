package at.dobiasch.mapreduce.framework;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.ArrayBlockingQueue;

public class RecordWriter
{
	private long ID;
	private boolean session;
	private long sessStart;
	private RandomAccessFile out;
	private String filename;
	
	private final byte[] keyValueSeparator;
	private Object sync= new Object();
	private boolean syncwait= false;
	
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
		this.filename= filename;
		this.session= false;
		this.sessStart= 0;
		
		try {
			this.keyValueSeparator = keyValueSeparator.getBytes(utf8);
		} catch (UnsupportedEncodingException uee) {
			throw new IllegalArgumentException("can't find " + utf8
					+ " encoding");
		}
	}
	
	public byte[] getKeyValueSeparator()
	{
		return keyValueSeparator;
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
	
	public synchronized void write(String key, String value) throws IOException
	{
		byte[] k= key.getBytes(utf8);
		byte[] v= value.getBytes(utf8);
		boolean nullKey= (k.length == 0);
		boolean nullValue= v.length == 0;
		
		/*synchronized( sync )
		{
			while( syncwait )
			{
				try
				{
					sync.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			syncwait= true;*/
			
			if (!nullKey)
				this.out.write(k);
			else
				System.out.println("Empty key");
			
			if (!(nullKey || nullValue))
				this.out.write(keyValueSeparator);
			else
				System.out.println("Empty key --> no kVS");
			
			if (!nullValue)
				this.out.write(v);
			
			this.out.write(newline);
			/*
			syncwait= false;
			sync.notifyAll();
		}*/
	}
	
	public void close() throws IOException
	{
		this.out.close();
	}
	
	public String getFilename()
	{
		return this.filename;
	}
	
	public long getLength()
	{
		try {
			return this.out.length();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return 0;
	}
}
