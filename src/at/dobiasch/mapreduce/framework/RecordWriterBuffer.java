package at.dobiasch.mapreduce.framework;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class RecordWriterBuffer
{
	private BlockingQueue<RecordWriter> q;
	private int size;
	
	public RecordWriterBuffer(int size, String template, 
			SysFileHandler sysh, String keyValueSeperator) throws IOException
	{
		this.size= size + 2;
		q= new ArrayBlockingQueue<RecordWriter>(this.size);
		for(int i= 0; i < this.size; i++)
		{
			q.add(new RecordWriter(sysh.addFile(String.format(template, i)),keyValueSeperator));
		}
		System.out.println(this + " added " + this.size + " files");
	}
	
	public RecordWriter get() throws InterruptedException
	{
		RecordWriter h= q.take();
		System.out.println(this + " 1 file taken");
		return h;
	}
	
	public void put(RecordWriter writer)
	{
		System.out.println(this + " 1 file added");
		q.add(writer);
	}
	
	public void closeAll() throws InterruptedException, IOException
	{
		for(int i= 0; i < this.size; i++)
		{
			RecordWriter w= this.q.take();
			w.close();
		}
	}
}
