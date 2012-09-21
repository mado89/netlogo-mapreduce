package at.dobiasch.mapreduce.framework;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;

public class RecordReader
{
	private RandomAccessFile in;
	private String filename;
	private long fsize;
	
	public int recs;
	
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
		this.fsize= writer.getLength();
		writer.close();
		this.keyValueSeparator= new String(writer.getKeyValueSeparator(), utf8);
		try {
			this.in= new RandomAccessFile(filename, "rw");
		} catch (FileNotFoundException e) {
			// This case should never happen since we create it from an existing one
			e.printStackTrace();
		}
		recs= 0;
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
		/*
		this.out.write(key.getBytes(utf8));
		this.out.write(keyValueSeparator);
		this.out.write(value.getBytes(utf8));
		this.out.write(newline);
		 */
		// Create a large buffer
		byte[] buffer= new byte[10000];
		int i= 0;
		try {
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
			if(ret.length < 2)
			{
				String[] reth= new String[2];
				reth[0]= null;
				reth[1]= ret[0];
				ret= reth;
			}
			/*for(i= 0; i < ret.length; i++)
				System.out.println(i + ": " + ret[i]);*/
			buffer= null;
			recs++;
			return ret;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
}
