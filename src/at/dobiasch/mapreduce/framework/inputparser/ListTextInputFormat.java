package at.dobiasch.mapreduce.framework.inputparser;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.nlogo.api.ExtensionException;
import org.nlogo.api.LogoListBuilder;

import at.dobiasch.mapreduce.framework.LogoObject;
import at.dobiasch.mapreduce.framework.controller.Data;

public class ListTextInputFormat implements IInputParser
{
	private String sep;
	private LogoObject vals;
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
			
			// System.out.println(data.ID + " " + new String(b));
			if( sep.equals("\n") )
			{
				LogoObject o= new LogoObject();
				o.set(new String(b).replaceAll("\\r|\\n", ""));
				this.vals= o;
			}
			else
			{
				LogoListBuilder list = new LogoListBuilder();
				String[] vals= new String(b).split(sep);
				b= null;
				for(int i= 0; i < vals.length; i++)
					list.add(vals[i].replaceAll("\\r|\\n", ""));
				
				// if( vals.length > 0)
				// 	System.out.println(data.ID + ": " + data.src + " " + data.start + " " + data + " " + vals[0].replaceAll("\\r|\\n", ""));
				// System.out.println("running " + data.key + " " + data.start + " " + data.end + " " + vals.length);
				
				LogoObject o= new LogoObject();
				o.set(list.toLogoList());
				this.vals= o;
			}
			
			String[] h= data.src.split("/");
			if( h.length > 1 )
				this.key= h[h.length - 1];
			else
				this.key= data.src;
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			throw new ExtensionException(e);
		} catch (IOException e) {
			e.printStackTrace();
			throw new ExtensionException(e);
		}
	}

	@Override
	public LogoObject getValues()
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
		ListTextInputFormat parser= new ListTextInputFormat();
		parser.init(this.sep);
		return parser;
	}	
}
