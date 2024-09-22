package test;

import org.apache.commons.lang3.StringUtils;

/**
 * This program shows StringUtils' regular expression based remove() method.
 */
class StringUtilsPerformanceTest {
    // StringUtils.remove - using regex, slow
    private static void testRemoveStrStr() {
        System.out.println("warming up remove(str, str)");
        for (int i = 0; i < 100000; i++) {
            String str = "\"6DFD3ILSNA7GMM\"";
            String result = StringUtils.remove(str, "\"");
        }

        long startTime = System.nanoTime();
        String str = "\"6DFD3ILSNA7GMM\"";
        String result = StringUtils.remove(str, "\"");
        System.out.println("result: " + result);
        System.out.println("remove(str, str) took "+(System.nanoTime()-startTime)+"nano seconds");
    }

    // StringUtils.remove - using char substitution is faster than using regex
    private static void testRemoveStrChar() {
        System.out.println("warming up remove(str, char)");
        for (int i = 0; i < 100000; i++) {
            String str = "\"6DFD3ILSNA7GMM\"";
            String result = StringUtils.remove(str, '\"');
        }

        long startTime = System.nanoTime();
        String str = "\"6DFD3ILSNA7GMM\"";
        String result = StringUtils.remove(str, '\"');
        System.out.println("result: " + result);
        System.out.println("remove(str, char) took "+(System.nanoTime()-startTime)+"nano seconds");
    }
    public static void main(String[] args) {
        testRemoveStrStr();
        testRemoveStrChar();
    }
}