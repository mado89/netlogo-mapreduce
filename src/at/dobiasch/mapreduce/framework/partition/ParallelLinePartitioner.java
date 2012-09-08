package at.dobiasch.mapreduce.framework.partition;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;

public class ParallelLinePartitioner extends BaseParallelPartitioner
{

	public ParallelLinePartitioner(String path, String sysdir)
			throws NoSuchAlgorithmException, IOException
	{
		super(path, sysdir);
		BufferedReader reader= new BufferedReader(is);
		DataInputStream in = new DataInputStream(fstream);
		  BufferedReader br = new BufferedReader(new InputStreamReader(in));
	}

	
	/*
	 * BufferedReader in= new BufferedReader(new FileReader(file));
				// while( in.)
				String line;
				while((line= in.readLine()) != null)
				{
					//s+= line;
					// mapt.perform(context, margs);
					if( line.length() > 0 )
						h.add(line.replace("\"", "\\\""));
				}
				in.close();
	 */
}
