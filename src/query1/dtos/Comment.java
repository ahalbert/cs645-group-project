package query1.dtos;
/**
 * 
 * @author klimzaporojets
 *
 */
public class Comment {

	private int userIdFrom; 
	private int userIdTo; 
//	private int numberOfComments; 
	
//	public int getNumberOfComments() {
//		return numberOfComments;
//	}
//	public void setNumberOfComments(int numberOfComments) {
//		this.numberOfComments = numberOfComments;
//	}
	public int getUserIdFrom() {
		return userIdFrom;
	}
	public void setUserIdFrom(int userIdFrom) {
		this.userIdFrom = userIdFrom;
	}
	public int getUserIdTo() {
		return userIdTo;
	}
	public void setUserIdTo(int userIdTo) {
		this.userIdTo = userIdTo;
	}
	
	@Override
	public boolean equals(Object obj)
	{
		Comment objToCompare = (Comment)obj;
		if( 
				this.getUserIdFrom() == objToCompare.getUserIdFrom() && 
				this.getUserIdTo() == objToCompare.getUserIdTo())
		{
			return true; 
		}
		return false; 
	}
	
    @Override
    public int hashCode() {
    	//discussion on this kind of hashcodes in: http://stackoverflow.com/questions/11742593/what-is-the-hashcode-for-a-custom-class-having-just-two-int-properties
    	int hash = 17;
        hash = hash * 31 + userIdFrom;
        hash = hash * 31 + userIdTo;
        return hash;
    }	
}
