package dws.app;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @基本功能:   智能分词
 * @program:Gmall_DW
 * @author: B1ue
 * @createTime:2025-04-18 10:39:29
 **/

public class IkUtil {
    public static List<String> analyzeSplit(String s) {
        List<String> result = new ArrayList<String>();
        // String => Reader

        Reader reader = new StringReader(s);
        // 智能分词
        // max_word
        IKSegmenter ikSegmenter = new IKSegmenter(reader, true);

        try {
            Lexeme next = ikSegmenter.next();
            while (next != null) {
                String word = next.getLexemeText();
                result.add(word);
                next = ikSegmenter.next();
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return result;
}

}
