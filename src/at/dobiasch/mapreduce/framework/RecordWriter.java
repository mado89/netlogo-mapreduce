package at.dobiasch.mapreduce.framework;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;

public class RecordWriter
{
	private long sID;
	private boolean session;
	private long sessStart;
	private RandomAccessFile out;
	private String filename;
	private boolean sessw; // write out session information
	
	private final byte[] keyValueSeparator;
	private boolean opened;
	private int recsw;
	// private boolean debug;
	// private Object sync= new Object();
	// private boolean syncwait= false;
	
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
		this.opened= true;
		
		try {
			this.keyValueSeparator = keyValueSeparator.getBytes(utf8);
		} catch (UnsupportedEncodingException uee) {
			throw new IllegalArgumentException("can't find " + utf8
					+ " encoding");
		}
		// this.debug=false;
		this.sessw= false;
	}
	
	public RecordWriter(String filename, String keyValueSeparator) throws IOException
	{
		File f= new File(filename);
		File dir= f.getParentFile();
		if( !dir.exists() )
			dir.mkdirs();
		this.out = new RandomAccessFile(filename, "rw");
		this.out.setLength(0);
		this.out.seek(0);
		this.filename= filename;
		this.session= false;
		this.sessStart= 0;
		this.opened= true;
		
		try {
			this.keyValueSeparator = keyValueSeparator.getBytes(utf8);
		} catch (UnsupportedEncodingException uee) {
			throw new IllegalArgumentException("can't find " + utf8
					+ " encoding");
		}
		// this.debug= false;
		this.sessw= false;
	}
	
	/*public RecordWriter(String filename, String keyValueSeparator, boolean debug) throws IOException
	{
		this.out = new RandomAccessFile(filename, "rw");
		this.out.seek(0);
		this.out.setLength(0);
		this.filename= filename;
		this.session= false;
		this.sessStart= 0;
		this.opened= true;
		
		try {
			this.keyValueSeparator = keyValueSeparator.getBytes(utf8);
		} catch (UnsupportedEncodingException uee) {
			throw new IllegalArgumentException("can't find " + utf8
					+ " encoding");
		}
		this.debug= debug;
		this.sessw= false;
	}*/

	public byte[] getKeyValueSeparator()
	{
		return keyValueSeparator;
	}
	
	/**
	 * 
	 * @param ID at the current moment id can be ignored
	 */
	public void startSession(long ID)
	{
		if ( this.session == true )
				throw new IllegalStateException("Session was allready started");
		this.sID= ID;
		this.session= true;
		this.recsw= 0;
	}
	
	public void endSession() throws IOException
	{
		this.session= false;
		
		if( recsw == 0 && sessw )
			this.write(null, null);
		recsw= 0;
		this.sessStart= this.out.getFilePointer();
	}
	
	public void writeSessionInfo(boolean write)
	{
		this.sessw= write;
	}
	
	public boolean isWritingSessionInfo()
	{
		return this.sessw;
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
		this.session= false;
	}
	
	public synchronized void write(String key, String value) throws IOException
	{
		byte[] k= null;
		byte[] v= null;
		boolean nullKey, nullValue;
		
		// if( debug )
		// 	System.out.println("Write " + filename + " " + key + " " + value);
		
		if( key != null )
		{
			k= key.getBytes(utf8);
			nullKey= (k.length == 0);
		}
		else
			nullKey= true;
		if( value != null )
		{
			v= value.getBytes(utf8);
			nullValue= v.length == 0;
		}
		else
			nullValue= true;
		
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
			
			if( this.sessw )
			{
				this.out.write(("" + sID).getBytes());
				this.out.write(keyValueSeparator);
			}
			
			if (!nullKey)
				this.out.write(k);
			// else
			// 	System.out.println("Empty key");
			
			if (!(nullKey || nullValue))
				this.out.write(keyValueSeparator);
			// else
			// 	System.out.println("Empty key --> no kVS");
			
			if (!nullValue)
				this.out.write(v);
			
			this.out.write(newline);
			/*
			syncwait= false;
			sync.notifyAll();
		}*/
		recsw++;
	}
	
	public void close() throws IOException
	{
		this.out.close();
		this.opened= false;
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

	public boolean isOpen() {
		return this.opened;
	}
}
