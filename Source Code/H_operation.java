package Dcore;
/* Input: A list of sorted non-negative number(small to big)
 * Output:H-index of input
 * Author: LIAO Xuankun
 */

public class H_operation {
	
	public static int h_operation(int []neighbor_degree) {//to use a static method, there is no need to new a object, just H_operation.h_operation
		int len = neighbor_degree.length;
		if(len==0||neighbor_degree[len-1]==0) {
			return 0;
		}
		
		int left = 0;
		int right = len-1;
		
		//binary search is used to looking for the subscript i
		while(left<right) {
			
			int mid= left +(right-left) / 2;
			if(len-mid>neighbor_degree[mid]) {
				// mid is too small 
				left = mid+1;
			}else {
				// to make mid smaller
				right = mid;
			}
		}
		return len-left;
	}

}
