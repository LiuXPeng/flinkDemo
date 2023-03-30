package source;

import java.util.Scanner;

/**
 * @version 1.0.0
 * @title: Test
 * @projectName flinkDemo
 * @description: TODO
 * @date： 2023-03-30 19:33
 */


public class Test {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        String tag = "2";
        while (!"1".equals(tag)) {
            tag = scanner.nextLine();
            System.out.println(tag);
        }
    }
}
