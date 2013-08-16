package at.dobiasch.mapreduce.framework;

public class Accumulator {
	private String svalue;
	private double dvalue;
	private boolean bvalue;
	private int type= 0;
	
	public Accumulator()
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
		}
		
		return null;
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
		}
		return null;
	}

	public Accumulator copy()
	{
		Accumulator accum= new Accumulator();
		
		accum.type= this.type;
		accum.svalue= this.svalue;
		accum.dvalue= this.dvalue;
		accum.bvalue= this.bvalue;
		
		return accum;
	}
}
