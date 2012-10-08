package at.dobiasch.mapreduce.framework.partition;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.NoSuchAlgorithmException;

import at.dobiasch.mapreduce.framework.SysFileHandler;

public class ParallelLinePartitioner extends BaseParallelPartitioner
{
	private BufferedReader in;
	// private String ret;
	private byte buf[];
	private int lines;
	private boolean endNotReached;
	private int length;
	
	public ParallelLinePartitioner(String path, SysFileHandler sysfileh, Integer blocksize)
			throws NoSuchAlgorithmException, IOException
	{
		super(path, sysfileh,blocksize);
		DataInputStream din= new DataInputStream(is);
		in= new BufferedReader(new InputStreamReader(din));
		lines= 0;
		endNotReached= true;
		buf= new byte[10000];
	}
	
	protected Object read()
	{
		try {
			int i= 0;
			buf[i++]= (byte) in.read();
			while( buf[i-1] != '\n' && buf[i-1] != -1)
				buf[i++]= (byte) in.read();
			// ret= in.readLine();
			// System.out.println("read: " + in + " bytes: " + i);
			if( buf[i-1] == -1 )
			{
				endNotReached= false;
				i--;
			}
			lines++;
			length= i;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return buf;
	}
	
	protected boolean doRead()
	{
		return endNotReached;
	}
	
	protected void update()
	{
		if( length > 0 )
		{
			complete.update(buf, 0, length);
			position+= length;
		}
	}
	
	protected boolean split()
	{
		// System.out.println("split? " + lines + " blk" + blocksize);
		/*if( lines > 0 && ret != null )
			return (lines % blocksize == 0);
		else
			return false;*/
		if( lines > 0 )
			return true;
		else
			return false;
	}
	
	protected void writePartition() throws IOException
	{
		// System.out.print("Partition: " + position + " " + new String(buf,0,length));
		out.write("" + position + "\n");
		this.partitions ++ ;
	}
}
