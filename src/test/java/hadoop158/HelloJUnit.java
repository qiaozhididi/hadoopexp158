package hadoop158;

import org.junit.Test;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

public class HelloJUnit {
    @Test
    public void test1() {
        //第1个参数时期待值，第2个参数是我们需要测试的值
        assertEquals(2, 1 + 1);//测试期望两者相等
        assertEquals("你好", "你" + "好");//测试期望两者相等
        assertFalse(3 == (1 + 1));//测试结果期望为 false
        assertNotEquals("帅哥", "帅锅");//测试期望两者不相等
    }

    public void test2() {
        String str1 = "1ll1ll1l11ll1l";
        String str2 = "1ll1ll1ll1ll1l";
        assertEquals(str1, equals(str2));
        assertEquals(str1, str2);
    }
}

