package com.matthew.gmall.realtime.dws.util;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashSet;
import java.util.Set;

/**
 * ClassName: IkUtil
 * Package: com.matthew.gmall.realtime.dws.util
 * Description:
 *
 * @Author Matthew-马之秋
 * @Create 2024/6/23 11:41
 * @Version 1.0
 */
public class IkUtil {
    public static Set<String> split(String str){
        HashSet<String> result = new HashSet<>();
        StringReader reader = new StringReader(str);
        //智能分词
        IKSegmenter ikSegmenter = new IKSegmenter(reader, true);

        try {
            Lexeme next = ikSegmenter.next();
            while (next != null){
                String word = next.getLexemeText();
                result.add(word);
                next = ikSegmenter.next();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    // public static void main(String[] args) {
    //     String str = "我爱自然语言处理和中文分词";
    //     Set<String> stringSet = split(str);
    //     for (String s : stringSet) {
    //         System.out.println(s);
    //     }
    // }
}
