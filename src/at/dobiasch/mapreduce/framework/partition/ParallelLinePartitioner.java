package at.dobiasch.mapreduce.framework.partition;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.NoSuchAlgorithmException;

public class ParallelLinePartitioner extends BaseParallelPartitioner
{
	private BufferedReader in;
	private String ret;
	private int lines;
	
	public ParallelLinePartitioner(String path, String sysdir, Integer blocksize)
			throws NoSuchAlgorithmException, IOException
	{
		super(path, sysdir,blocksize);
		DataInputStream din= new DataInputStream(is);
		in= new BufferedReader(new InputStreamReader(din));
		lines= 0;
	}
	
	protected Object read()
	{
		try {
			ret= in.readLine();
			lines++;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return ret;
	}
	
	protected boolean doRead()
	{
		return (ret != null);
	}
	
	protected void update()
	{
		if( ret != null )
		{
			int h= ret.length() + 1;
			complete.update((ret + "\n").getBytes(), 0, h);
			position+= h;
		}
	}
	
	protected boolean split()
	{
		return (lines % blocksize == 0);
	}
	
	protected void writePartition() throws IOException
	{
		System.out.println(lines + " " + blocksize);
		out.write("" + position + "\n");
	}
}
