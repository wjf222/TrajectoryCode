package job;

import indexs.IndexRange;
import indexs.commons.Window;
import indexs.z2.XZ2SFC;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class TestXZ2 {
    public static void main(String[] args) {
        XZ2SFC sfc = new XZ2SFC((short) 10,-180.0,180.0,-90.0,90.0);
        Window window = new Window(1,-20,25,45);
        List<Window> tmp = new ArrayList<>();
        tmp.add(window);
        List<IndexRange> ranges = sfc.ranges(tmp, Optional.of(10));
        for (IndexRange range:ranges) {
//            System.out.printf("length lower=%d upper=%d \n",lengthInt(range.lower),lengthInt(range.upper));
            System.out.printf("lower=%d upper=%d \r\n",range.lower,range.upper);
        }
        long index = sfc.index(1, -5, 45, 45, true);
        System.out.println(index);
    }

    private static int lengthInt(long x) {
        int ans = 0;
        while(x != 0) {
            ans += 1;
            x = x/10;
        }
        return ans;
    }
}
