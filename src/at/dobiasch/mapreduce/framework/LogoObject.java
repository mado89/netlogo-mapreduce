package at.dobiasch.mapreduce.framework;

import org.nlogo.api.LogoList;
import org.nlogo.api.LogoListBuilder;

public class LogoObject {
	private String svalue;
	private double dvalue;
	private boolean bvalue;
	private LogoList lvalue;
	private int type= 0;
	private double hval;
	
	public LogoObject()
	{
		type= 0;
	}
	
	public String toString() 
	{
		switch( type )
		{
			case 0:
				return "<>";
			case 1:
				return svalue;
			case 2:
				return "" + dvalue;
			case 3:
				return "" + (bvalue ? '1' : '0');
			case 4:
				return "" + lvalue.toString();
		}
		return null;
	}
	
	public void set(Object o)
	{
		if( o.getClass().equals(java.lang.String.class) )
		{
			type= 1;
			svalue= (String) o;
		}
		else if( o.getClass().equals(java.lang.Double.class) )
		{
			type= 2;
			dvalue= (Double) o;
		}
		else if( o.getClass().equals(java.lang.Boolean.class) )
		{
			type= 3;
			bvalue= (Boolean) o;
		}
		else if( o.getClass().equals(org.nlogo.api.LogoList.class) )
		{
			type= 4;
			lvalue= (LogoList) o;
		}
	}
	
	public String export()
	{
		switch(type)
		{
			case 0:
				throw new IllegalStateException();
			case 1:
				return "S" + svalue;
			case 2:
				return "D" + dvalue;
			case 3:
				return "B" + (bvalue ? '1' : '0');
			case 4:
				return "L" + lvalueToLogo();
		}
		
		return null;
	}
	
	private boolean isNumeric(String str)
	{
		try {
			this.hval= Double.parseDouble(str);
		} catch (NumberFormatException nfe) {
			return false;
		}
		return true;
	}
	
	private LogoList fromString(String value)
	{
		LogoListBuilder lb= new LogoListBuilder();
		int s= 2;
		int i= 2;
		boolean string= false;
		while( value.charAt(i) != ']' )
		{
			if( value.charAt(i) == ' ' )
			{
				if( i > s )
				{
					String val= value.substring(s, i);
					if( isNumeric(val) )
						lb.add(this.hval);
					else if( val.equals("true") )
						lb.add(true);
					else if( val.equals("false") )
						lb.add(false);
					//TODO: List
				}
				s= i + 1;
			} else if( value.charAt(i) == '\"' )
			{
				if( string == true )
				{
					lb.add(value.substring(s+1, i));
					s= i + 1;
					string= false;
				}
				else
					string= true;
			}
			i++;
		}
		
		return lb.toLogoList();
	}
	
	public void createFromString(String string)
	{
		if( isNumeric(string) ) {
			type= 2;
			dvalue= hval;
		} else if ( string.charAt(0) == '[' ) {
			type= 4;
			String[] vals= string.split(", ");
			LogoListBuilder lb= new LogoListBuilder();
			
			vals[0]= vals[0].substring(1);
			
			for(int i= 1; i < vals.length - 1;i++)
				lb.add(vals[i]);
			
			if( vals.length > 0 )
				lb.add(vals[vals.length - 1].substring(0, vals[vals.length - 1].length() - 1));
			
			lvalue= lb.toLogoList();
		} else if( string.equals("true") ) {
			type= 3;
			bvalue= true;
		} else if( string.equals("false") ) {
			type= 3;
			bvalue= false;
		} else {
			type= 1;
			svalue= string;
		}
	}
	
	public void importValue(String value)
	{
		char t= value.charAt(0);
		switch( t )
		{
			case 'S':
				type= 1;
				svalue= value.substring(1);
				break;
			case 'D':
				type= 2;
				dvalue= Double.parseDouble(value.substring(1));
				break;
			case 'B':
				type= 3;
				if( value.charAt(1) == '1' )
					bvalue= true;
				else
					bvalue= false;
			case 'L':
				type= 4;
				lvalue= fromString(value);
		}
	}
	
	public String toLogo()
	{
		switch( type )
		{
			case 0:
				throw new IllegalStateException();
			case 1:
				return "\"" + svalue + "\"";
			case 2:
				return "" + dvalue;
			case 3:
				return (bvalue ? "True" : "False");
			case 4:
				return lvalueToLogo();
		}
		return null;
	}
	
	public Object getObject()
	{
		switch( type )
		{
			case 0:
				throw new IllegalStateException();
			case 1:
				return svalue;
			case 2:
				return dvalue;
			case 3:
				return bvalue;
			case 4:
				return lvalue;
		}
		return null;
	}
	
	private String lvalueToLogo()
	{
		String ret= "[";
		java.util.Iterator<Object> it= lvalue.iterator();
		while( it.hasNext() )
		{
			Object o= it.next();
			if( o.getClass() == String.class )
				ret+= "\"" + o + "\" ";
			else
				ret+= o.toString() + " ";
		}
		
		ret+= "]";
		
		return ret;
	}

	public LogoObject copy()
	{
		LogoObject accum= new LogoObject();
		
		accum.type= this.type;
		accum.svalue= this.svalue;
		accum.dvalue= this.dvalue;
		accum.bvalue= this.bvalue;
		accum.lvalue= this.lvalue;
		
		return accum;
	}
	/*
	public static void main(String[] args) {
		LogoListBuilder lb= new LogoListBuilder();
		lb.add("test");
		lb.add(true);
		LogoObject a= new LogoObject();
		LogoObject b= new LogoObject();
		a.set(lb.toLogoList());
		System.out.println(a.toLogo());
		System.out.println(a.export());
		b.importValue(a.export());
		System.out.println(b.toString());
		System.out.println(b.toLogo());
		
		LogoObject o= new LogoObject();
		o.createFromString("[/home/martin/DA/input5/4.txt, /home/martin/DA/input5/2.txt, /home/martin/DA/input5/1.txt, /home/martin/DA/input5/0.txt, /home/martin/DA/input5/3.txt]");
		System.out.println(o);
		
	}*/
}
