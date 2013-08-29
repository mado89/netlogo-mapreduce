package at.dobiasch.mapreduce.framework;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

public class RecordReader
{
	private RandomAccessFile in;
	private String filename;
	private long fsize;
	private boolean sessinfo;
	
	private long recStart;
	
	// private List<String[]> read;
	
	private static final String utf8 = "UTF-8";
	private final String keyValueSeparator;
	private static final byte newline;
	static {
	      try {
	        newline = "\n".getBytes(utf8)[0];
	      } catch (UnsupportedEncodingException uee) {
	        throw new IllegalArgumentException("can't find " + utf8 + " encoding");
	      }
	    }
	
	/**
	 * Create a Reader from a writer, and closes the writer
	 * @throws IOException 
	 */
	public RecordReader(RecordWriter writer) throws IOException
	{
		this.filename= writer.getFilename();
		this.fsize= writer.getLength(); // TODO: soll gehen auch wenn file zu ist
		this.sessinfo= writer.isWritingSessionInfo();
		if( writer.isOpen() )
			writer.close();
		this.keyValueSeparator= new String(writer.getKeyValueSeparator(), utf8);
		try {
			this.in= new RandomAccessFile(filename, "rw");
		} catch (FileNotFoundException e) {
			// This case should never happen since we create it from an existing one
			e.printStackTrace();
		}
	}

	public boolean hasRecordsLeft()
	{
		try {
			return this.in.getFilePointer() < this.fsize;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
	}

	public String[] readRecord()
	{
		// Create a large buffer
		byte[] buffer= new byte[10000];
		
		int i= 0;
		try {
			recStart= this.in.getFilePointer();
			
			buffer[i]= this.in.readByte();
			while(buffer[i] != newline)
			{
				// System.out.println("read " + i + " " + buffer[i]);
				i++;
				buffer[i]= this.in.readByte();
			}
			// System.out.println("read " + i + " " + buffer[i]);
			String h= new String(buffer,0,i,utf8);
			// int splitpos= h.indexOf(keyValueSeparator);
			// System.out.println(h);
			String[] ret= h.split(keyValueSeparator);
			if( this.sessinfo )
			{
				if(ret.length < 3)
				{
					// This case means:
					/// we have a session information but not key and value
					/// in case we have two items -> session and key
					/// otherwise we only have the session information
					String[] reth= new String[3];
					reth[0]= ( ret.length == 2 ) ? ret[1] : null;
					reth[1]= null;
					reth[2]= ret[0];
					ret= reth;
				}
				else
				{
					String tmp;
					tmp= ret[0];
					for(i= 1; i < ret.length; i++)
						ret[i-1]= ret[i];
					ret[ret.length-1]= tmp;
				}
			}
			else if(ret.length < 2)
			{
				String[] reth= new String[2];
				reth[1]= null;
				reth[0]= ret[0];
				ret= reth;
			}
			/*for(i= 0; i < ret.length; i++)
				System.out.println(i + ": " + ret[i]);*/
			buffer= null;
			return ret;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	private void unReadRecord() throws IOException
	{
		this.in.seek(recStart);
	}
	
	public List<String[]> readSession()
	{
		if( this.sessinfo == false)
			throw new UnsupportedOperationException("Can't read Session from a file without session info");
		
		List<String[]> ret= new ArrayList<String[]>();;
		String ses;
		
		if( hasRecordsLeft() )
		{
			String[] rec= readRecord();
			boolean loop;
			
			ses= rec[rec.length-1];
			
			loop= true;
			ret.add(rec);
			
			if( hasRecordsLeft() )
			{
				while( hasRecordsLeft() && loop)
				{
					rec= readRecord();
					if( ses.equals(rec[rec.length-1]))
					{
						ret.add(rec);
						loop= true;
					}
					else
					{
						loop= false;
						try {
							unReadRecord();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				}
			}
		}
		
		if( ret.size() > 0)
			return ret;
		else
			return null;
	}

	public void close() throws IOException {
		this.in.close();
	}

	public String getFilename() {
		return this.filename;
	}

	public String getKeyValueSeparator() {
		return this.keyValueSeparator;
	}
}
