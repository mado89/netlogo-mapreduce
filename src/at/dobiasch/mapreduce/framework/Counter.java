package at.dobiasch.mapreduce.framework;

public class Counter
{
	private int value= 0;
	
	public Counter(int val)
	{
		this.value= val;
	}
	
	public Counter()
	{
		this.value= 0;
	}

    public synchronized int getValue()
    {
        return value;
    }

    public synchronized int add()
    {
        return ++value;
    }
    
    public synchronized int dec()
    {
    	return --value;
    }
}
