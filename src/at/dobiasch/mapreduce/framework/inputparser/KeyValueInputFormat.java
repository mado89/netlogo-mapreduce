package at.dobiasch.mapreduce.framework.inputparser;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.nlogo.api.ExtensionException;
import org.nlogo.api.LogoList;
import org.nlogo.api.LogoListBuilder;

import at.dobiasch.mapreduce.framework.controller.Data;

public class KeyValueInputFormat implements IInputParser
{
	private String sep;
	private LogoList vals;
	private String key;

	@Override
	public void init(String valueseperator)
	{
		this.sep= valueseperator;
	}

	@Override
	public void parseInput(Data data) throws ExtensionException
	{
		RandomAccessFile in;
		try
		{
			in = new RandomAccessFile(data.src, "r");
			byte[] b= new byte[(int) (data.end - data.start)];
			
			in.seek(data.start);
			in.read(b);
			
			in.close();
			
			LogoListBuilder list = new LogoListBuilder();
			String h= new String(b);
			int split= h.indexOf(this.sep);
			// System.out.println(h.replaceAll("\\r|\\n", "") + " '" + sep + "'");
			if( split != -1 )
			{
				this.key= h.substring(0, split);
				split+= this.sep.length();
			}
			else
			{
				this.key= "";
				split= 0;
			}
			
			list.add(h.substring(split).replaceAll("\\r|\\n", ""));
			
			// System.out.println("running " + data.key + " " + data.start + " " + data.end + " " + vals.length);
			
			this.vals= list.toLogoList();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			throw new ExtensionException(e);
		} catch (IOException e) {
			e.printStackTrace();
			throw new ExtensionException(e);
		}
	}

	@Override
	public LogoList getValues()
	{
		return this.vals;
	}

	@Override
	public String getKey()
	{
		return this.key;
	}

	@Override
	public IInputParser newInstance()
	{
		KeyValueInputFormat parser= new KeyValueInputFormat();
		parser.init(this.sep);
		return parser;
	}	
}
