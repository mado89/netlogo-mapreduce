package at.dobiasch.mapreduce.framework;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class RecordWriterBuffer
{
	private BlockingQueue<RecordWriter> q;
	private Counter size;
	private String keyValueSeperator;
	
	public RecordWriterBuffer(int size, String template, 
			SysFileHandler sysh, String keyValueSeperator) throws IOException
	{
		this.size= new Counter(size);
		this.keyValueSeperator= keyValueSeperator;
		q= new ArrayBlockingQueue<RecordWriter>(this.size.getValue());
		for(int i= 0; i < this.size.getValue(); i++)
		{
			q.add(new RecordWriter(sysh.addFile(String.format(template, i)),keyValueSeperator));
		}
		// System.out.println(this + " added " + this.size + " files");
	}
	
	public RecordWriter get() throws InterruptedException
	{
		RecordWriter h= q.take();
		this.size.dec();
		// System.out.println(this + " 1 file taken");
		return h;
	}
	
	public void put(RecordWriter writer)
	{
		// System.out.println(this + " 1 file added");
		this.size.add();
		q.add(writer);
	}
	
	public void closeAll()
	{
		for(int i= 0; i < this.size.getValue(); i++)
		{
			try
			{
				RecordWriter w= this.q.take();
				w.close();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		this.size= new Counter(0);
	}

	public boolean hasFiles()
	{
		return !this.q.isEmpty();
	}
	
	public int getSize()
	{
		return this.size.getValue();
	}
	
	public String getKeyValueSeperator()
	{
		return this.keyValueSeperator;
	}
}
