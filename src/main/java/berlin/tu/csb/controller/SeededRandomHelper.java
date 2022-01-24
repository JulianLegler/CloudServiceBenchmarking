package berlin.tu.csb.controller;

import org.apache.commons.codec.binary.Hex;

import java.util.Random;
import java.util.UUID;

public class SeededRandomHelper {
    public Random seededRandom;
    public static final String[] alphabet =  {"a","b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z", "A","B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z"};


    public SeededRandomHelper() {}

    public SeededRandomHelper(long seed) {
        seededRandom = new Random(seed);
    }

    public UUID getUUID() {

        byte[] randomBytes = new byte[16];
        seededRandom.nextBytes(randomBytes);
        randomBytes[6]  &= 0x0f;  /* clear version        */
        randomBytes[6]  |= 0x40;  /* set to version 4     */
        randomBytes[8]  &= 0x3f;  /* clear variant        */
        randomBytes[8]  |= 0x80;  /* set to IETF variant  */
        return UUID.nameUUIDFromBytes(randomBytes);
    }
    public float getFloatBetween(float start, float end) {
        return start + ((end - start) * seededRandom.nextFloat());
    }

    public long getLongBetween(long start, long end) {
        return  start + ((long)(seededRandom.nextDouble() * (end - start)));
    }

    public String getStringWithLength(int minLength, int maxLength) {
        int actualLenght = minLength + seededRandom.nextInt(maxLength-minLength);
        String randomString = "";
        for (int i = 0; i <= actualLenght; i++) {
            randomString += alphabet[seededRandom.nextInt(alphabet.length)];
        }
        return randomString;
    }

    public int getIntWithLength(int minLength, int maxLength) {
        int actualLenght = minLength + seededRandom.nextInt(maxLength-minLength);
        String randomIntegerString = "";
        for (int i = 0; i < actualLenght; i++) {
            randomIntegerString += seededRandom.nextInt(10);
        }
        return Integer.parseInt(randomIntegerString);
    }

    public int getIntBetween(int start, int end) {
        return  start + ((int)(seededRandom.nextDouble() * ((end - start) + 1 )));
    }



}
