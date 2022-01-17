package berlin.tu.csb.controller;

import java.util.Date;
import java.util.Random;

public class SeededRandomHelper {
    public static Random seededRandom = new Random(2122 + new Date().getTime());
    public static String alphabet[] =  {"a","b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z", "A","B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z"};


    public SeededRandomHelper() {}



    public static float getFloatBetween(float start, float end) {
        return start + ((end - start) * seededRandom.nextFloat());
    }

    public static long getLongBetween(long start, long end) {
        return  start + ((long)(seededRandom.nextDouble() * (end - start)));
    }

    public static String getStringWithLength(int minLength, int maxLength) {
        int actualLenght = minLength + seededRandom.nextInt(maxLength-minLength);
        String randomString = "";
        for (int i = 0; i <= actualLenght; i++) {
            randomString += alphabet[seededRandom.nextInt(alphabet.length)];
        }
        return randomString;
    }

    public static int getIntWithLength(int minLength, int maxLength) {
        int actualLenght = minLength + seededRandom.nextInt(maxLength-minLength);
        String randomIntegerString = "";
        for (int i = 0; i < actualLenght; i++) {
            randomIntegerString += seededRandom.nextInt(10);
        }
        return Integer.valueOf(randomIntegerString);
    }

    public static int getIntBetween(int start, int end) {
        return  start + ((int)(seededRandom.nextDouble() * (end - start)));
    }



}
