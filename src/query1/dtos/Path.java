package query1.dtos;
/**
 * 
 * @author klimzaporojets
 *
 */
public class Path {
	private int distanceToOrigin;
	private Integer parent; 
	
	public Path(int distanceToOrigin, Integer parent)
	{
		this.distanceToOrigin = distanceToOrigin; 
		this.parent = parent;
	}
	public int getDistanceToOrigin() {
		return distanceToOrigin;
	}
	public void setDistanceToOrigin(int distanceToOrigin) {
		this.distanceToOrigin = distanceToOrigin;
	}
	public Integer getParent() {
		return parent;
	}
	public void setParent(Integer parent) {
		this.parent = parent;
	}
	
}
